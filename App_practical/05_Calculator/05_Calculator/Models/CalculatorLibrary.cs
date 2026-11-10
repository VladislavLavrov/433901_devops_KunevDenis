namespace Calculator.Models;

public static class CalculatorLibrary
{
    public static double CalculateOperation(double num1, double num2, Operation operation)
    {
        if (!double.IsFinite(num1) || !double.IsFinite(num2))
            throw new ArgumentException("Операнды должны быть конечными числами.");
        var result = operation switch
        {
            Operation.Add => num1 + num2,
            Operation.Subtract => num1 - num2,
            Operation.Multiply => num1 * num2,
            Operation.Divide when num2 != 0 => num1 / num2,
            Operation.Divide => throw new ArgumentException("Деление на ноль запрещено."),
            _ => throw new ArgumentException("Неизвестная операция.")
        };
        if (!double.IsFinite(result))
            throw new ArgumentException("Результат выходит за допустимый диапазон.");
        return result;
    }
}
