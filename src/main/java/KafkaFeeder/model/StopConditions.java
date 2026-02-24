package KafkaFeeder.model;

/**
 * Defines the stopping criteria for an automatic producer process.
 *
 * <p>The process can stop based on the number of records produced
 * or the total elapsed execution time.</p>
 *
 * @param recordProduced the maximum number of records to be produced before stopping
 * @param elapsedTimeMs the maximum allowed execution time in milliseconds before stopping
 */
public record StopConditions(int recordProduced, long elapsedTimeMs) {}
