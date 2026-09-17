using System.Collections.Specialized;
using System.Globalization;
using System.Net;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks.Dataflow;
using meterEmul;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MQTTnet;
using MQTTnet.Exceptions;

namespace Sunspec2Mqtt;

internal class Sunspec2MqttService : BackgroundService
{
    private readonly IConfiguration config;
    private string mqttTopicRoot;
    private IMqttClient? mqttClient;
    private readonly ILogger<Sunspec2MqttService> log;
    private IPAddress ipAddress;

    public Sunspec2MqttService(IConfiguration config, ILogger<Sunspec2MqttService> log)
    {
        this.config = config;
        this.log = log;

        ipAddress = IPAddress.Parse("192.168.10.73");
    }

    async Task<IMqttClient> Reconnect(CancellationToken cancellationToken)
    {
        var login = config["mqtt:username"];
        var password = config["mqtt:password"];
        var server = config["mqtt:broker"];

        var mqttOptions = new MqttClientOptionsBuilder()
            .WithClientId("Sunspec2Mqtt")
            .WithTcpServer(server)
            .WithCredentials(login, password)
            .Build();

        var mqttFactory = new MqttClientFactory();
        var client = mqttFactory.CreateMqttClient();

        while (true)
        {
            try
            {
                log.LogInformation("Connecting to MQTT broker");
                var connectResult = await client.ConnectAsync(mqttOptions, cancellationToken);

                if (connectResult.ResultCode != MqttClientConnectResultCode.Success)
                {
                    log.LogError($"Connect error: {connectResult.ResultCode}");
                    await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
                    continue;
                }
            }
            catch (Exception ex)
            {
                log.LogError(ex.Message);
                log.LogError("Connection failed. Retrying in 5 sec.");
                await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
                continue;
            }

            client.ApplicationMessageReceivedAsync += async args =>
            {
                var topic = args.ApplicationMessage.Topic;
                var str = Encoding.ASCII.GetString(args.ApplicationMessage.Payload);
                log.LogInformation($"Received to {topic}: {str}");

                // Sunspec/Fronius/serial-num/Storage/OutWRte/set

                if (topic.StartsWith(mqttTopicRoot))
                {
                    var fields = topic.Split('/');
                    var model = modelSpecs.SingleOrDefault(m => m.Name == fields[3]);
                    var field = model?.Fields.SingleOrDefault(f => f.Name == fields[4]);

                    log.LogDebug($"model {fields[3]} field {fields[4]}");

                    if (model != null && field != null)
                    {
                        log.LogInformation($"Field {model.Name} -> {field.Name}");
                        field.TryParse(str);

                        log.LogInformation($"New value: {field.NewValue[0]}");

                        await updates.SendAsync((fields[3], field));
                    }
                }
            };

            return client;
        }
    }

    private bool autoConfigPerformed = false;

    string UnitOfMeasurement(SunspecUnit unit)
    {
        switch (unit)
        {
            case SunspecUnit.A:
                return "A";
            case SunspecUnit.W:
                return "W";
            case SunspecUnit.VA:
                return "VA";
            case SunspecUnit.V:
                return "V";
            case SunspecUnit.Percentage:
                return "%";
            default:
                return string.Empty;
        }
    }

    async Task HomeAssistantAutoConfig(CancellationToken cancel)
    {
        var commonModel = modelSpecs.Single(m => m.Name == "Common");

        var deviceSerialNumber = commonModel["SN"];
        var uniqueId = $"Sunspec2Mqttv1" + deviceSerialNumber;
        var device = new
        {
            manufacturer = commonModel["Mn"],
            name = "Sunspec Inverter",
            model = commonModel["Md"],
            identifiers = new[]
            {
                uniqueId
            }
        };

        var components = new Dictionary<string, object>();

        foreach (var model in modelSpecs)
        {
            foreach (var field in model.Fields)
            {
                var id = $"{uniqueId}_{model.Name}_{field.Name}";

                object config = field.IsNumberType && field.Writable ? new
                {
                    platform = "number",
                    state_topic = $"{mqttTopicRoot}/{model.Name}/{field.Name}",
                    command_topic = $"{mqttTopicRoot}/{model.Name}/{field.Name}/cmd",
                    value_template = "{{ value }}",
                    unique_id = id,
                    name = field.DisplayName,
                    unit_of_measurement = UnitOfMeasurement(field.Unit),
                    min = field.Min != null ? field.Min.ToString() : "",
                    max = field.Max != null ? field.Max.ToString() : ""
                } : 
                    new
                {
                    platform = "sensor",
                    state_topic = $"{mqttTopicRoot}/{model.Name}/{field.Name}",
                    value_template = "{{ value }}",
                    unique_id = id,
                    name = field.DisplayName,
                    unit_of_measurement = UnitOfMeasurement(field.Unit)
                };

                components[$"{model.Name}_{field.Name}"] = config;
            }
        }

        var lowPriceConfig = new
        {
            platform = "binary_sensor",
            state_topic = mqttTopicRoot + "/lowPrice",
            //state_class = "total_increasing",
            //device_class = "water",
            availability = new
            {
                topic = mqttTopicRoot + "/lowPrice/availability"
            },
            value_template = "{{ value }}",
            //unit_of_measurement = "m\u00b3",
            name = "Low price",
            unique_id = uniqueId + "lowPrice",
        };

        var highPriceConfig = new
        {
            platform = "binary_sensor",
            state_topic = mqttTopicRoot + "/highPrice",
            //state_class = "total_increasing",
            //device_class = "water",
            availability = new
            {
                topic = mqttTopicRoot + "/highPrice/availability"
            },
            value_template = "{{ value }}",
            //unit_of_measurement = "m\u00b3",
            name = "High price",
            unique_id = uniqueId + "highPrice",
        };

        var configTopic = $"homeassistant/device/{uniqueId}/config";

        var deviceDiscoveryPayload = new
        {
            device,
            origin = new
            {
                name = "EasyWatt",
                sw_version = "1",
                url = "https://example.com"
            },
            components
/*            components = new
            {
                LowPriceOn = lowPriceConfig,
                HighPriceOn = highPriceConfig
            }*/
        };

        var serialized = JsonSerializer.Serialize(deviceDiscoveryPayload);

        log.LogDebug(serialized);

        await mqttClient.PublishAsync(new MqttApplicationMessage
        {
            Topic = configTopic,
            PayloadSegment = Encoding.UTF8.GetBytes(serialized),
            Retain = true
        }, cancel);

        autoConfigPerformed = true;
    }

    /*async Task Publish(bool low, bool high, bool online, CancellationToken cancel)
    {
        while (true)
        {
            try
            {
                await mqttClient.PublishStringAsync(mqttTopicRoot + "/lowPrice/availability", online ? "online" : "offline",
                    cancellationToken: cancel);
                if (online)
                {
                    await mqttClient.PublishStringAsync(mqttTopicRoot + "/lowPrice", low ? "ON" : "OFF",
                        cancellationToken: cancel);
                }

                await mqttClient.PublishStringAsync(mqttTopicRoot + "/highPrice/availability", online ? "online" : "offline",
                    cancellationToken: cancel);
                if (online)
                {
                    await mqttClient.PublishStringAsync(mqttTopicRoot + "/highPrice", high ? "ON" : "OFF", cancellationToken: cancel);
                }

                break;
            }
            catch (MqttClientNotConnectedException ex)
            {
                log.LogError(ex, "Publish failed");
                mqttClient = await Reconnect(cancel);
            }
        }

    }*/

    public class SunspecModel
    {
        public ushort Address { get; set; }

        public int Identifier { get; set; }
        public string Name { get; set; }

        public SunspecField[] Fields { get; set; }

        public string this[string index]
        {
            get
            {
                var field = Fields.Single(f => f.Name == index);
                return field.ValueToString();
            }
        }
    }
    /*
    "1: OFF
    2: EMPTY
    3: DISCHAGING
    4: CHARGING
    5: FULL
    6: HOLDING
    7: TESTING


    The status TESTING is used during battery calibration or service charge.
    "
                */
    public enum ChargeStatus
    {
        Off = 1,
        Empty = 2,
        Discharging = 3,
        Charging = 4,
        Full = 5,
        Holding = 6,
        Testing = 7
    }

    public class SunspecStorage : SunspecModel
    {
        public ushort WChaMax { get; set; }
        public ushort WChaGra { get; set; }
        public ushort WDisChaGra { get; set; }
        public ushort StorCtl_Mod { get; set; }
        /// <summary>
        /// Setpoint for minimum reserve for storage as a percentage of the nominal maximum storage.
        /// </summary>
        public ushort MinRsvPct { get; set; }

        /// <summary>
        /// Currently available energy as a percent of the capacity rating.
        /// </summary>
        public ushort ChaState { get; set; }

        public ushort ChaSt { get; set; }

        // Percent of max charging rate
        public short InWRte { get; set; }

        /// <summary>
        /// Percent of max discharge rate.
        /// </summary>
        public short OutWRte { get; set; }

        /// <summary>
        /// 0: PV (Charging from grid disabled)
        /// 1: GRID(Charging from grid enabled)
        /// </summary>
        public ushort ChaGriSet { get; set; }
    }

    public class SunspecInverter : SunspecModel
    {
        // AC Power
        public float W { get; set; }
        // Line frequency
        public float Hz { get; set; }

        // Cabinet temperature (C)
        public float TmpCab { get; set; }
    }

    public class SunspecControls : SunspecModel
    {
        public ushort Conn_WinTms { get; set; }
        public ushort Conn_RvrtTms { get; set; }
        public ushort Conn { get; set; }
        public ushort WMaxLimPct { get; set; }
        public ushort WMaxLimPct_WinTms { get; set; }
        public ushort WMaxLimPct_RvrtTms { get; set; }
        public ushort WMaxLimPct_RmpTms { get; set; }
        public ushort WMaxLim_Ena { get; set; }
        public ushort OutPFSet { get; set; }
        public ushort OutPFSet_WinTms { get; set; }
        public ushort OutPFSet_RvrtTms { get; set; }
        public ushort OutPFSet_RmpTms { get; set; }
        public ushort OutPFSet_Ena { get; set; }
        public ushort VArWMaxPct { get; set; }
        public ushort VArMaxPct { get; set; }
        public ushort VArAvalPct { get; set; }
        public ushort VArPct_WinTms { get; set; }
        public ushort VArPct_RvrtTms { get; set; }
        public ushort VArPct_RmpTms { get; set; }
        public ushort VArPct_Mod { get; set; }
        public ushort VArPct_Ena { get; set; }
        public ushort WMaxLimPct_SF { get; set; }
        public ushort OutPFSet_SF { get; set; }
        public ushort VArPct_SF { get; set; }
    }

    public enum SunspecFieldType
    {
        Int16,
        UInt16,
        Enum16,
        Bitfield16,
        String
    }

    public enum SunspecUnit
    {
        None,
        Percentage,
        V,
        VA,
        A,
        W,
        Seconds
    }

    public class SunspecField
    {
        public SunspecField()
        {
            Length = 1;
        }

        string FromBigEndianUShorts(ushort[] source, Encoding encoding)
        {
            // Each ushort contributes two bytes
            byte[] bytes = new byte[source.Length * 2];

            for (int i = 0; i < source.Length; i++)
            {
                ushort val = source[i];
                // Big-endian means the high byte comes first
                bytes[i * 2]     = (byte)(val >> 8);  // high
                bytes[i * 2 + 1] = (byte)(val & 0xFF); // low
            }

            // Find a zero byte if you want C-style null termination
            int length = Array.IndexOf(bytes, (byte)0);
            if (length < 0) length = bytes.Length;

            return encoding.GetString(bytes, 0, length);
        }

        public required string Name { get; set; }
        public required string DisplayName { get; set; }
        public string Description { get; set; }
        public bool Writable { get; set; }
        public decimal? Min { get; set; }
        public decimal? Max { get; set; }

        public bool IsNumberType => Type == SunspecFieldType.UInt16 || Type == SunspecFieldType.Enum16 ||
                                    Type == SunspecFieldType.Int16 || Type == SunspecFieldType.Bitfield16;

        public SunspecFieldType Type { get; set; }
        public SunspecUnit Unit { get; set; }

        // Number of 16-bit registers (if Type == String)
        public int Length { get; set; }

        public ushort[] CurrentValue { get; set; }

        public ushort[] NewValue { get; set; }

        public string ValueToString()
        {
            switch (Type)
            {
                case SunspecFieldType.String:
                    {
                    return FromBigEndianUShorts(CurrentValue, Encoding.ASCII);
                    }
                case SunspecFieldType.Int16:
                    if (Unit == SunspecUnit.Percentage)
                    {
                        return ((short)CurrentValue[0] / 100.0m).ToString(CultureInfo.InvariantCulture);
                    }
                    return ((short)CurrentValue[0]).ToString();
                default:
                    if (Unit == SunspecUnit.Percentage)
                    {
                        return (CurrentValue[0] / 100.0m).ToString(CultureInfo.InvariantCulture);
                    }

                    return CurrentValue[0].ToString();
            }
        }

        public bool TryParse(string str)
        {
            bool res;

            switch (Type)
            {
                case SunspecFieldType.Int16:
                    if (Unit == SunspecUnit.Percentage)
                    {
                        res = decimal.TryParse(str, out var dresult);
                        if (res)
                        {
                            NewValue = [(ushort)(short)(dresult * 100)];
                        }
                    }
                    else
                    {
                        res = short.TryParse(str, out var sresult);
                        if (res)
                        {
                            NewValue = [(ushort)sresult];
                        }
                    }

                    break;
                case SunspecFieldType.String:
                    throw new NotSupportedException();
                default:
                    if (Unit == SunspecUnit.Percentage)
                    {
                        res = decimal.TryParse(str, out var dresult);
                        if(res)
                        {
                            NewValue = [(ushort)(dresult * 100)];
                        }
                    }
                    else
                    {
                        res = ushort.TryParse(str, out var uresult);
                        if (res)
                        {
                            NewValue = [uresult];
                        }
                    }

                    break;
            }

            return res;
        }
    }

    private SunspecModel[] modelSpecs = [
        new()
        {
            Name = "Common",
            Identifier = 1,
            Fields = [
                new SunspecField
                {
                    Name = "Mn",
                    DisplayName = "Manufacturer",
                    Length = 16,
                    Type = SunspecFieldType.String,
                    Writable = false
                },
                new SunspecField
                {
                    Name = "Md",
                    DisplayName = "Device",
                    Writable = false,
                    Length = 16,
                    Type = SunspecFieldType.String
                },
                new SunspecField
                {
                    Name = "Opt",
                    DisplayName = "Options",
                    Writable = false,
                    Length = 8,
                    Type = SunspecFieldType.String
                },
                new SunspecField
                {
                    Name = "Vr",
                    DisplayName = "Software version",
                    Writable = false,
                    Length = 8,
                    Type = SunspecFieldType.String
                },
                new SunspecField
                {
                    Name = "SN",
                    DisplayName = "Serial number",
                    Writable = false,
                    Length = 16,
                    Type = SunspecFieldType.String
                },
                new SunspecField
                {
                    Name = "DA",
                    DisplayName = "Modbus device address",
                    Writable = false,
                    Type = SunspecFieldType.UInt16
                },
            ]
        },
        new()
        {
            Name = "Storage",
            Identifier = 124,
            Fields = [
                new SunspecField
                {
                    Name = "WChaMax",
                    DisplayName = "Maximum charge",
                    Writable = false,
                    Type = SunspecFieldType.UInt16,
                    Unit = SunspecUnit.W
                },
                new SunspecField
                {
                    Name = "WChaGra",
                    DisplayName = "Maximum charging rate",
                    Writable = false,
                    Type = SunspecFieldType.UInt16
                },
                new SunspecField
                {
                    Name = "WDisChaGra",
                    DisplayName = "Maximum discharge rate",
                    Writable = false,
                    Type = SunspecFieldType.UInt16
                },
                new SunspecField
                {
                    Name = "StoCtl_Mod",
                    DisplayName = "Storage control mode",
                    Writable = true,
                    Type = SunspecFieldType.Bitfield16,
                    Min = 0,
                    Max = 3
                },
                new SunspecField
                {
                    Name = "VAChaMax",
                    DisplayName = "Maximum charging VA",
                    Writable = false,
                    Unit = SunspecUnit.VA,
                    Type = SunspecFieldType.UInt16
                },
                new SunspecField
                {
                    Name = "MinRsvPct",
                    DisplayName = "Minimum reserve percentage",
                    Writable = true,
                    Type = SunspecFieldType.UInt16,
                    Unit = SunspecUnit.Percentage,
                    Min = 0,
                    Max = 100
                },
                new SunspecField
                {
                    Name = "ChaState",
                    DisplayName = "Available energy",
                    Writable = false,
                    Type = SunspecFieldType.UInt16,
                    Unit = SunspecUnit.Percentage
                },
                new SunspecField
                {
                    Name = "StorAval",
                    DisplayName = "Available storage",
                    Writable = false,
                    Type = SunspecFieldType.UInt16
                },
                new SunspecField
                {
                    Name = "InBatV",
                    DisplayName = "Internal battery voltage",
                    Writable = false,
                    Type = SunspecFieldType.UInt16,
                    Unit = SunspecUnit.V
                },
                new SunspecField
                {
                    Name = "ChaSt",
                    DisplayName = "Charge status",
                    Writable = false,
                    Type = SunspecFieldType.Enum16,
                },
                new SunspecField
                {
                    Name = "OutWRte",
                    DisplayName = "Maximum discharge rate",
                    Writable = true,
                    Type = SunspecFieldType.Int16,
                    Unit = SunspecUnit.Percentage,
                    Min = -100,
                    Max = 100
                },
                new SunspecField
                {
                    Name = "InWRte",
                    DisplayName = "Maximum charge rate",
                    Writable = true,
                    Type = SunspecFieldType.Int16,
                    Unit = SunspecUnit.Percentage,
                    Min = -100,
                    Max = 100
                },
                new SunspecField
                {
                    Name = "InOutWRte_WinTms",
                    DisplayName = "Time window for charge/discharge rate change",
                    Writable = false,
                    Type = SunspecFieldType.UInt16,
                    Unit = SunspecUnit.Seconds
                },
                new SunspecField
                {
                    Name = "InOutWRte_RvrtTms",
                    DisplayName = "Timeout period for charge/discharge rate",
                    Writable = true,
                    Type = SunspecFieldType.UInt16,
                    Unit = SunspecUnit.Seconds,
                    Min = 0,
                    Max = 28800
                },
                new SunspecField
                {
                    Name = "InOutWRte_RmpTms",
                    DisplayName = "Ramp time",
                    Writable = false,
                    Type = SunspecFieldType.UInt16,
                    Unit = SunspecUnit.Seconds
                },
                new SunspecField
                {
                    Name = "ChaGriSet",
                    DisplayName = "Enable charge from grid",
                    Writable = true,
                    Type = SunspecFieldType.Enum16,
                    Min = 0,
                    Max = 1
                },
            ]
        }
    ];

    float FromFloat32(UInt32 float32)
    {
        return BitConverter.UInt32BitsToSingle(float32);
    }

    async Task PublishField(SunspecModel model, SunspecField field)
    {
        await mqttClient.PublishStringAsync(mqttTopicRoot + $"/{model.Name}/{field.Name}", field.ValueToString());
    }

    async Task ExtractModelData(SunspecModel model, ushort[] dataBuffer, bool force = false)
    {
        var bufIx = 0;

        for (int i = 0; i < model.Fields.Length; i++)
        {
            var field = model.Fields[i];

            if (dataBuffer.Length < i + 1)
            {
                log.LogWarning($"Expecting more data ({i}), data length is {dataBuffer.Length}");
                continue;
            }

            field.CurrentValue = new ushort[field.Length];
            Array.Copy(dataBuffer, bufIx, field.CurrentValue, 0, field.Length);
            bufIx += field.Length;
        }
    }

    async Task PublishModel(SunspecModel model)
    {
        foreach(var field in model.Fields)
        {
            await PublishField(model, field);
        }
    }

    private bool haRegistered = false;

    async Task UpdateModels(CancellationToken cancel)
    {
        using var modbusClient = new ModbusMaster(502, ipAddress, log);
        await modbusClient.Start(cancel);

        var regs = (await modbusClient.ReadMultipleRegisters(40000, 2)).ToArray();
        if (regs[0] != 0x5375 || regs[1] != 0x6e53)
        {
            throw new Exception("Sunspec identifier not found.");
        }

        ushort nextModelAddress = 40002;

        bool continueSunspecScan = true;

        while (continueSunspecScan)
        {
            regs = (await modbusClient.ReadMultipleRegisters(nextModelAddress, 2)).ToArray();

            if (regs[0] == 0xFFFF)
            {
                break;
            }

            var modelLen = regs[1];

            var modelData =
                (await modbusClient.ReadMultipleRegisters((ushort)(nextModelAddress + 2), modelLen)).ToArray();

            var modelIdentifier = regs[0];

            var modelSpec = modelSpecs.SingleOrDefault(s => s.Identifier == modelIdentifier);

            if (modelSpec == null)
            {
                log.LogTrace($"Unknown model {regs[0]}");
            }
            else
            {
                modelSpec.Address = nextModelAddress;

                //log.LogDebug($"Model {modelSpec.Name} address is {modelSpec.Address}");

                await ExtractModelData(modelSpec, modelData, true);
            }

            nextModelAddress = (ushort)(nextModelAddress + 2 + modelLen);
        }

        var common = modelSpecs.Single(m => m.Name == "Common");
        var manufacturer = common.Fields.Single(f => f.Name == "Mn").ValueToString();

        if (!string.IsNullOrEmpty(manufacturer))
        {
            mqttTopicRoot = $"Sunspec/{manufacturer}/{common["SN"]}";

            foreach (var model in modelSpecs)
            {
                await PublishModel(model);
            }

            if (!autoConfigPerformed)
            {
                await HomeAssistantAutoConfig(cancel);
            }
        }
    }

    async Task Subscribe()
    {
        //await mqttClient.PublishStringAsync(mqttTopicRoot + $"/{model.Name}/{field.Name}", field.ValueToString());

        foreach (var model in modelSpecs)
        {
            foreach (var field in model.Fields)
            {
                if (field.Writable)
                {
                    await mqttClient.SubscribeAsync(mqttTopicRoot + $"/{model.Name}/{field.Name}/cmd");
                }
            }
        }
    }

    private BufferBlock<(string, SunspecField)> updates = new BufferBlock<(string, SunspecField)>();

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            if (mqttClient != null)
            {
                mqttClient.Dispose();
                mqttClient = null;
            }

            mqttClient = await Reconnect(stoppingToken);
            //await HomeAssistantAutoConfig(stoppingToken);

            await UpdateModels(stoppingToken);

            await Subscribe();

            try
            {
                Task delayTask = Task.Delay(TimeSpan.FromMinutes(1), stoppingToken);
                Task<(string, SunspecField)> updateTask = updates.ReceiveAsync(stoppingToken);

                while (!stoppingToken.IsCancellationRequested)
                {
                    var completedTask = await Task.WhenAny(delayTask, updateTask);

                    if (completedTask == delayTask)
                    {
                        await delayTask;
                        await UpdateModels(stoppingToken);
                        delayTask = Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
                    }

                    if (completedTask == updateTask)
                    {
                        var update = await updateTask;
                        try
                        {
                            await UpdateField(update.Item1, update.Item2, stoppingToken);
                        }
                        catch (Exception)
                        {
                            if (!mqttClient.IsConnected)
                            {
                                mqttClient.Dispose();
                                mqttClient = await Reconnect(stoppingToken);
                            }
                            else
                            {
                                throw;
                            }
                        }

                        updateTask = updates.ReceiveAsync(stoppingToken);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                //await Publish(false, false, false, CancellationToken.None);
                return;
            }
            catch (MqttClientNotConnectedException)
            {

            }
        }
    }

    async Task UpdateField(string modelName, SunspecField field, CancellationToken cancel)
    {
        log.LogDebug($"Updatefield in model {modelName}");
        var model = modelSpecs.Single(m => m.Name == modelName);

        if (field.CurrentValue != field.NewValue)
        {
            using var modbusClient = new ModbusMaster(502, ipAddress, log);
            await modbusClient.Start(cancel);

            var address = (ushort)(model.Address + 2 + Array.IndexOf(model.Fields, field));
            var written = await modbusClient.WriteMultipleRegisters(address, field.NewValue);
            if (written != 1)
            {
                log.LogError($"Write to {address} failed.");
                return;
            }

            field.CurrentValue = field.NewValue;
            await PublishField(model, field);
        }
    }
}