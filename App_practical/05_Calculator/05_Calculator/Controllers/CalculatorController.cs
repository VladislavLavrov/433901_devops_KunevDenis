using Calculator.Data;
using Calculator.Models;
using Calculator.Services;
using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using System.Text.Json;

namespace Calculator.Controllers;

public class CalculatorController : Controller
{
    private readonly CalculatorContext _context;
    private readonly KafkaProducerService<Null, string> _producer;
    private readonly string _topic;

    public CalculatorController(CalculatorContext context,
        KafkaProducerService<Null, string> producer, IConfiguration config)
    {
        _context = context;
        _producer = producer;
        _topic = config["Kafka:TopicName"]
            ?? throw new InvalidOperationException("Kafka:TopicName is required.");
    }

    [HttpGet]
    public IActionResult Index() => View(_context.DataInputVariants
        .OrderByDescending(x => x.ID_DataInputVariant).Take(50).ToList());

    [HttpGet]
    public IActionResult Results() => PartialView("_Results", _context.DataInputVariants
        .OrderByDescending(x => x.ID_DataInputVariant).Take(50).ToList());

    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Calculate(double num1, double num2, Operation operation)
    {
        if (!ModelState.IsValid)
        {
            TempData["Message"] = "Проверьте введённые числа.";
            return RedirectToAction(nameof(Index));
        }
        try
        {
            // Validate before publishing; the consumer performs the actual calculation.
            CalculatorLibrary.CalculateOperation(num1, num2, operation);
        }
        catch (ArgumentException ex)
        {
            TempData["Message"] = ex.Message;
            return RedirectToAction(nameof(Index));
        }
        var input = new DataInputVariant
        {
            Operand_1 = num1, Operand_2 = num2, Type_operation = operation
        };
        try
        {
            await _producer.ProduceAsync(_topic,
                new Message<Null, string> { Value = JsonSerializer.Serialize(input) });
            TempData["Message"] = "Запрос отправлен в Kafka. Результат появится в таблице.";
        }
        catch (ProduceException<Null, string>)
        {
            TempData["Message"] = "Kafka недоступна. Попробуйте отправить запрос позже.";
        }
        return RedirectToAction(nameof(Index));
    }

    [HttpPost]
    public IActionResult Callback([FromBody] DataInputVariant inputData)
    {
        if (!ModelState.IsValid || inputData.ID_DataInputVariant != 0 || inputData.Result is null)
            return BadRequest();
        _context.DataInputVariants.Add(inputData);
        _context.SaveChanges();
        return Ok();
    }

    [HttpGet]
    public IActionResult Error() => Problem("Не удалось выполнить запрос. Подробности в журнале приложения.");
}
