package ru.integrotechnologies.octopus.library.rabbitmq.connection.producer;

import com.rabbitmq.client.AMQP;
import lombok.extern.log4j.Log4j2;
import ru.integrotechnologies.octopus.exceptions.OctopusException;
import ru.integrotechnologies.octopus.loggin.LogMessage;

import java.io.IOException;
import java.util.function.Consumer;

@Log4j2
public class RabbitMqProducerPoolWrapper implements RabbitMqProducerConnection {
    final RabbitMqProducerConnectionImpl connection;
    private boolean isClosed = false;
    private final Consumer<RabbitMqProducerConnectionImpl> closeDelegate;

    public RabbitMqProducerPoolWrapper(RabbitMqProducerConnectionImpl connection,
                                       Consumer<RabbitMqProducerConnectionImpl> closeDelegate) {

        this.closeDelegate = closeDelegate;
        this.connection = connection;

    }

    @Override
    public boolean checkReadiness() {
        if (!isClosed)
            return connection.checkReadiness();
        else {
            log.error(LogMessage.create("Attempt to call closed connection"));
            throw new OctopusException("Attempt to call closed connection");
        }
    }

    @Override
    public void riseUncommittedData() {
        if (!isClosed)
            connection.riseUncommittedData();
        else {
            log.error(LogMessage.create("Attempt to call closed connection"));
            throw new OctopusException("Attempt to call closed connection");
        }
    }

    @Override
    public boolean hasUncommittedData() {
        if (!isClosed)
            return connection.hasUncommittedData();
        else {
            log.error(LogMessage.create("Attempt to call closed connection"));
            throw new OctopusException("Attempt to call closed connection");
        }
    }

    @Override
    public String getName() {
        if (!isClosed)
            return connection.getName();
        else {
            log.error(LogMessage.create("Attempt to call closed connection"));
            throw new OctopusException("Attempt to call closed connection");
        }
    }


    @Override
    public void beginTransaction() {
        if (!isClosed)
            connection.beginTransaction();
        else {
            log.error(LogMessage.create("Attempt to call closed connection"));
            throw new OctopusException("Attempt to call closed connection");
        }
    }

    @Override
    public void rollbackTransaction() {
        if (!isClosed)
            connection.rollbackTransaction();
        else {
            log.error(LogMessage.create("Attempt to call closed connection"));
            throw new OctopusException("Attempt to call closed connection");
        }
    }

    @Override
    public void commitTransaction() {
        if (!isClosed)
            connection.commitTransaction();
        else {
            log.error(LogMessage.create("Attempt to call closed connection"));
            throw new OctopusException("Attempt to call closed connection");
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;

    }

    @Override
    public void close() throws IOException {
        isClosed = true;
        closeDelegate.accept(connection);
    }


    @Override
    public void post(String exchange,String routingKey,AMQP.BasicProperties basicProperties, byte[] body) {
        if (!isClosed)
            connection.post(exchange,routingKey,basicProperties, body);
        else {
            log.error(LogMessage.create("Attempt to call closed connection"));
            throw new OctopusException("Attempt to call closed connection");
        }
    }
}
