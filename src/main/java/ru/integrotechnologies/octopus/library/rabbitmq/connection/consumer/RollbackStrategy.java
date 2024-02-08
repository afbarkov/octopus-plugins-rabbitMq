package ru.integrotechnologies.octopus.library.rabbitmq.connection.consumer;

public enum RollbackStrategy {
    RETRY,
    DISCARD,
    DLQ

}
