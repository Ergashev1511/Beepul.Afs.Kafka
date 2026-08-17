namespace Beepul.Afs.Kafka.Abstractions
{
    public class PartialBatchFailure : Exception
    {
        public IReadOnlyDictionary<int, Exception> FailedIndices { get; }

        public PartialBatchFailure(IReadOnlyDictionary<int, Exception> failedIndices)
            : base($"Batch ichida {failedIndices.Count} ta element xato berdi")
        {
            FailedIndices = failedIndices;
        }
    }
}
