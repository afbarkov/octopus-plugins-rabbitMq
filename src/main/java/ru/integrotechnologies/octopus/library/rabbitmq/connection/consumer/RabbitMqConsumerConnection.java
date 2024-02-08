package ru.integrotechnologies.octopus.library.rabbitmq.connection.consumer;

import ru.integrotechnologies.octopus.internal.resources.SourceConnection;

public interface RabbitMqConsumerConnection extends SourceConnection {

    ConsumerRecord getRecord();

    DlqMessage getDlqMessage();
}
