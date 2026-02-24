package KafkaFeeder.model;

/**
 * Represents automatic execution options for a producer process.
 *
 * <p>This configuration defines how message production should behave
 * when running in automatic mode. It controls the execution interval
 * between operations and the conditions that determine when the process
 * must stop.</p>
 *
 * @param interval the time interval (in milliseconds) between each execution cycle
 * @param stopConditions the conditions that define when the automatic process must stop
 */
public record AutoOptions(long interval, StopConditions stopConditions) {}
