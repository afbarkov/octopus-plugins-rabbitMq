package ru.integrotechnologies.octopus.library.rabbitmq.connection.consumer;

import ru.integrotechnologies.octopus.exceptions.OctopusException;

import java.io.IOException;
import java.util.function.Consumer;

public class RabbitMqConsumerPoolWrapper implements RabbitMqConsumerConnection {
    final RabbitMqConsumerConnectionImpl connection;
    private boolean isClosed = false;
    private final Consumer<RabbitMqConsumerConnectionImpl> closeDelegate;

    public RabbitMqConsumerPoolWrapper(RabbitMqConsumerConnectionImpl connection,
                                       Consumer<RabbitMqConsumerConnectionImpl> closeDelegate) {

        this.closeDelegate = closeDelegate;
        this.connection = connection;

    }

    @Override
    public boolean checkReadiness() {
        if (!isClosed)
            return connection.checkReadiness();
        else
            throw new OctopusException("Attempt to call closed connection");
    }

    @Override
    public void riseUncommittedData() {
        if (!isClosed)
            connection.riseUncommittedData();
        else
            throw new OctopusException("Attempt to call closed connection");
    }

    @Override
    public boolean hasUncommittedData() {
        if (!isClosed)
            return connection.hasUncommittedData();
        else
            throw new OctopusException("Attempt to call closed connection");
    }

    @Override
    public String getName() {
        if (!isClosed)
            return connection.getName();
        else
            throw new OctopusException("Attempt to call closed connection");
    }


    @Override
    public void beginTransaction() {
        if (!isClosed)
            connection.beginTransaction();
        else
            throw new OctopusException("Attempt to call closed connection");
    }

    @Override
    public void rollbackTransaction() {
        if (!isClosed)
            connection.rollbackTransaction();
        else
            throw new OctopusException("Attempt to call closed connection");
    }

    @Override
    public void commitTransaction() {
        if (!isClosed)
            connection.commitTransaction();
        else
            throw new OctopusException("Attempt to call closed connection");
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
    public ConsumerRecord getRecord() {
        if (!isClosed)
            return connection.getRecord();
        else
            throw new OctopusException("Attempt to call closed connection");
    }

    @Override
    public DlqMessage getDlqMessage() {
        if (!isClosed)
            return connection.getDlqMessage();
        else
            throw new OctopusException("Attempt to call closed connection");
    }
}
