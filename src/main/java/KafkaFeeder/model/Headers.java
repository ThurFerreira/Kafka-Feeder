package KafkaFeeder.model;

import java.util.Map;

/**
 * Represents a collection of message headers.
 *
 * <p>Headers are key-value pairs that can be attached to a message
 * to provide additional metadata during message transmission.</p>
 *
 * @param headers a map containing header names as keys and their respective values
 */
public record Headers(Map<String, String> headers) {}
