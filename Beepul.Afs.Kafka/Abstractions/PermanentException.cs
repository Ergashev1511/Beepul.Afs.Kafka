namespace Beepul.Afs.Kafka.Abstractions
{
    public class PermanentException : Exception
    {
        public PermanentException(string message, Exception? inner = null)
            : base(message, inner) { }
    }
}
