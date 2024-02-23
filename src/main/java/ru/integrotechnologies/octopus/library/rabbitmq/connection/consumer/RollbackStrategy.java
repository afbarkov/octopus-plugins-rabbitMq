package org.lexna.octopus.library.rabbitmq.connection.consumer;

public enum RollbackStrategy {
    RETRY,
    DISCARD,
    DLQ

}
