package KafkaFeeder.Service.KafkaService;

/**
 * Thrown when a Kafka message could not be sent successfully.
 *
 * This exception indicates that the broker did not acknowledge
 * the message or an unexpected error occurred during sending.
 */
public class NotSentException extends RuntimeException {

    public NotSentException(String message) {
        super(message);
    }

    public NotSentException(String message, Throwable cause) {
        super(message, cause);
    }
}