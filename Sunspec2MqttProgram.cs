using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Sunspec2Mqtt
{
    internal class Sunspec2MqttProgram
    {
        static async Task Main(string[] args)
        {
            var builder =
                Host.CreateDefaultBuilder(args)
                    .ConfigureServices((hostContext, services) =>
                    {
                        services.AddSingleton(TimeProvider.System);

                        //services.AddLogging(builder => builder.AddFile("water2mqtt-{Date}.log", LogLevel.Trace));
                        services.AddHostedService<Sunspec2MqttService>();
                    })
                    .ConfigureAppConfiguration((hostingContext, config) =>
                    {
                        var env = hostingContext.HostingEnvironment;
                        config.AddCommandLine(args);
                        config.AddEnvironmentVariables();
                        config.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);
                        config.AddJsonFile($"appsettings.{env.EnvironmentName}.json", optional: true, reloadOnChange: true);
                        config.AddUserSecrets<Sunspec2MqttProgram>(optional: true);
                        config.AddEnvironmentVariables();
                    });

            await builder.Build().RunAsync();
        }
    }

}