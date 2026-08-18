namespace Beepul.Afs.Kafka.Abstractions;

/// <summary>
/// Signals that retrying the same event batch cannot succeed without changing its data.
/// Permanent failures are routed to the configured dead-letter topic.
/// </summary>
public sealed class PermanentException : Exception
{
    public PermanentException(string message, Exception? innerException = null)
        : base(message, innerException)
    {
    }
}
