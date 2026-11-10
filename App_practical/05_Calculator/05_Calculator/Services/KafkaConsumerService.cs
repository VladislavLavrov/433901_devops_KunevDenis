using Calculator.Data;
using Calculator.Models;
using Confluent.Kafka;
using System.Globalization;
using System.Net.Http.Json;
using System.Text.Json;

namespace Calculator.Services;

public sealed class KafkaConsumerService : BackgroundService
{
    private readonly IConsumer<Ignore, string> _consumer;
    private readonly IHttpClientFactory _clients;
    private readonly ILogger<KafkaConsumerService> _logger;
    private readonly string _topic;
    private readonly string _callback;

    public KafkaConsumerService(IConfiguration config, IHttpClientFactory clients,
        ILogger<KafkaConsumerService> logger)
    {
        var settings = new ConsumerConfig();
        config.GetSection("Kafka:ConsumerSettings").Bind(settings);
        settings.EnableAutoCommit = false;
        settings.EnableAutoOffsetStore = false;
        _consumer = new ConsumerBuilder<Ignore, string>(settings).Build();
        _clients = clients;
        _logger = logger;
        _topic = config["Kafka:TopicName"] ?? throw new InvalidOperationException("Kafka:TopicName is required.");
        _callback = config["Kafka:CallbackUrl"] ?? throw new InvalidOperationException("Kafka:CallbackUrl is required.");
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        Task.Run(() => ConsumeMessages(stoppingToken), stoppingToken);

    private async Task ConsumeMessages(CancellationToken cancellationToken)
    {
        _consumer.Subscribe(_topic);
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                ConsumeResult<Ignore, string> message;
                try { message = _consumer.Consume(cancellationToken); }
                catch (ConsumeException ex) when (!ex.Error.IsFatal)
                {
                    _logger.LogWarning(ex, "Kafka is not ready; retrying.");
                    await Task.Delay(2000, cancellationToken);
                    continue;
                }
                var input = JsonSerializer.Deserialize<DataInputVariant>(message.Message.Value)
                    ?? throw new InvalidOperationException("Empty Kafka message.");
                input.Result = CalculatorLibrary.CalculateOperation(
                    input.Operand_1, input.Operand_2, input.Type_operation)
                    .ToString(CultureInfo.InvariantCulture);
                // Retry this record before consuming another; never commit a failed save.
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var client = _clients.CreateClient();
                        using var response = await client.PostAsJsonAsync(_callback, input, cancellationToken);
                        response.EnsureSuccessStatusCode();
                        break;
                    }
                    catch (HttpRequestException ex)
                    {
                        _logger.LogWarning(ex, "Callback failed; retrying in 3 seconds.");
                        await Task.Delay(3000, cancellationToken);
                    }
                }
                cancellationToken.ThrowIfCancellationRequested();
                _consumer.Commit(message);
                _logger.LogInformation("Processed Kafka message: {Value}", message.Message.Value);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
        finally { _consumer.Close(); }
    }

    public override void Dispose()
    {
        _consumer.Dispose();
        base.Dispose();
    }
}
