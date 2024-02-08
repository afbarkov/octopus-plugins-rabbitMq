package ru.integrotechnologies.octopus.library.rabbitmq.connection.producer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import lombok.extern.log4j.Log4j2;
import ru.integrotechnologies.octopus.exceptions.OctopusSourceConnectionException;
import ru.integrotechnologies.octopus.library.rabbitmq.connection.consumer.ConsumerRecord;
import ru.integrotechnologies.octopus.loggin.LogMessage;
import ru.integrotechnologies.octopus.metrics.GaugeMetric;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

@Log4j2
public class RabbitMqProducerConnectionImpl implements RabbitMqProducerConnection {
    private final String factoryName;
    private final Connection connection;
    private final Channel channel;

    private final GaugeMetric lastSendDuration;
    private final Queue<ConsumerRecord> buffer = new ConcurrentLinkedDeque();

    final AtomicBoolean isClosed = new AtomicBoolean(false);
    final AtomicBoolean contaminatedConnection = new AtomicBoolean(false);
    private final AtomicBoolean uncommittedData = new AtomicBoolean(false);


    private ConsumerRecord currentRecord = null;


    public RabbitMqProducerConnectionImpl(String factoryName,
                                          Connection connection,
                                          Channel channel,
                                          GaugeMetric lastSendDuration) throws IOException {
        this.factoryName = factoryName;
        this.connection = connection;
        this.channel = channel;

        this.lastSendDuration = lastSendDuration;
    }

    @Override
    public synchronized boolean checkReadiness() {
        try {
            boolean result = channel.isOpen() && connection.isOpen();
            if (!result)
                contaminatedConnection.set(true);
            return result;

        } catch (Exception e) {
            contaminatedConnection.set(true);
            return false;
        }

    }

    @Override
    public void riseUncommittedData() {
        uncommittedData.set(true);
    }

    @Override
    public boolean hasUncommittedData() {
        //consumer cant have uncommitted outgoing data;
        return uncommittedData.get();
    }

    @Override
    public String getName() {
        return factoryName;
    }


    @Override
    public void beginTransaction() {
        try {
            channel.txSelect();

        } catch (Exception e) {
            contaminatedConnection.set(true);
            log.error(LogMessage.create("Error starting transaction")
                    .addParam("stacktrace", e)
            );
            throw new OctopusSourceConnectionException("Error starting  transaction", e);
        }
    }

    @Override
    public void rollbackTransaction() {
        try {
            channel.txRollback();

        } catch (Exception e) {
            contaminatedConnection.set(true);
            log.error(LogMessage.create("Error rollback transaction")
                    .addParam("stacktrace", e)
            );
            throw new OctopusSourceConnectionException("Error rolling back message", e);
        }

    }

    @Override
    public void commitTransaction() {
        try {
            channel.txCommit();

        } catch (Exception e) {
            contaminatedConnection.set(true);
            log.error(LogMessage.create("Error committing message")
                    .addParam("stacktrace", e)
            );
            throw new OctopusSourceConnectionException("Error committing message", e);
        }

        log.info("Transaction committed");
    }

    @Override
    public boolean isClosed() {
        return isClosed.get();
    }


    @Override
    public synchronized void close() throws IOException {
        try {
            if (channel.isOpen())
                channel.close();
        } catch (Exception ignored) {

        }
        try {


            if (connection.isOpen())
                connection.close();
        } catch (Exception ignored) {
        }
        isClosed.set(true);
    }


    @Override
    public void post(String exchange,String routingKey,AMQP.BasicProperties basicProperties, byte[] body) {
        try {
        long t1=System.currentTimeMillis();
            channel.basicPublish(exchange, routingKey,
                    basicProperties,
                    body);
            lastSendDuration.set(System.currentTimeMillis()-t1);
            log.info(new LogMessage("Message being sent")
                    .addParam("exchange", exchange)
                    .addParam("routingKey", routingKey));
        } catch (Exception e) {
            contaminatedConnection.set(true);
            log.error(LogMessage.create("Error publishing message")
                    .addParam("stacktrace", e)
            );
            throw new OctopusSourceConnectionException("Error publishing message", e);
        }


    }
}
