using System.Text;
using Microsoft.Azure.ServiceBus;

namespace WorkerServiceBusQueue;

public class Worker : IHostedService
{
    private readonly ILogger<Worker> _logger;
    private readonly ExecutionParameters _executionParameters;
    private readonly QueueClient _client;
    
    public Worker(ILogger<Worker> logger,
        ExecutionParameters executionParameters)
    {
        logger.LogInformation(
            $"Queue = {executionParameters.Queue}");

        _logger = logger;
        _executionParameters = executionParameters;
        _client = new (
            _executionParameters.ConnectionString,
            _executionParameters.Queue,
            ReceiveMode.ReceiveAndDelete);
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "Iniciando o processamento de mensagens...");
        _client.RegisterMessageHandler(
            async (message, stoppingToken) =>
            {
                await ProcessMessage(message);
            }
            ,
            new MessageHandlerOptions(
                async (e) =>
                {
                    await HandleError(e);
                }
            )
        );

        _logger.LogInformation("Aguardando mensagens...");

        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken stoppingToken)
    {
        await _client.CloseAsync();
        _logger.LogInformation(
            "Conexao com o Azure Service Bus fechada!");
    }

    private Task ProcessMessage(Message message)
    {
        var conteudo = Encoding.UTF8.GetString(message.Body);
        _logger.LogInformation("[Nova mensagem recebida] " + conteudo);
        return Task.CompletedTask;
    }

    private Task HandleError(ExceptionReceivedEventArgs e)
    {
        _logger.LogError("[Falha] " +
            e.Exception.GetType().FullName + " " +
            e.Exception.Message);
        return Task.CompletedTask;
    }
}