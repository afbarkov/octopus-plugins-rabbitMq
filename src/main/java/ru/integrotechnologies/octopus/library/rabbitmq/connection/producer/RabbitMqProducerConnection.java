package org.lexna.octopus.library.rabbitmq.connection.producer;

import com.rabbitmq.client.AMQP;
import org.lexna.octopus.internal.resources.SourceConnection;

public interface RabbitMqProducerConnection extends SourceConnection {

    void post(String exchange,String routingKey,AMQP.BasicProperties basicProperties, byte[] body);
}
