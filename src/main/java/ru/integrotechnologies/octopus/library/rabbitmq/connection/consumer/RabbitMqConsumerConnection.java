package org.lexna.octopus.library.rabbitmq.connection.consumer;

import org.lexna.octopus.internal.resources.SourceConnection;

public interface RabbitMqConsumerConnection extends SourceConnection {

    ConsumerRecord getRecord();

    DlqMessage getDlqMessage();
}
