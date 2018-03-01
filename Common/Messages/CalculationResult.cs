using System;

namespace Common.Messages
{
    public class CalculationResult
    {
        public CalculationResult(int number, int result, Guid workerId)
        {
            Result = result;
            WorkerId = workerId;
            Number = number;
        }

        public int Number { get; }
        public int Result { get; }
        public Guid WorkerId { get; }
    }
}