namespace Common.Messages
{
    public class CalculateFibonacci
    {
        public CalculateFibonacci(int number)
        {
            Number = number;
        }

        public int Number { get; }
    }
}