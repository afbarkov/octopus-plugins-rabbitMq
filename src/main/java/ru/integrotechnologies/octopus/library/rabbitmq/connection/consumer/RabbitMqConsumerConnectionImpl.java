package org.lexna.octopus.library.rabbitmq.connection.consumer;

import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.ShutdownSignalException;
import lombok.extern.log4j.Log4j2;
import org.lexna.octopus.common.service.DlqRecord;
import org.lexna.octopus.common.service.DlqService;
import org.lexna.octopus.exceptions.OctopusSourceConnectionException;
import org.lexna.octopus.loggin.LogMessage;
import org.lexna.octopus.metrics.GaugeMetric;
import org.lexna.octopus.manager.TransactionManagerException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

@Log4j2
public class RabbitMqConsumerConnectionImpl implements RabbitMqConsumerConnection {
    private final String factoryName;

    private final String sourceUrl;
    private final RollbackStrategy rollbackStrategy;
    private final DlqService dlqService;
    private final Connection connection;
    private final Channel channel;
    private final GaugeMetric lastReceiveTime;
    private final Queue<ConsumerRecord> buffer = new ConcurrentLinkedDeque();

    final AtomicBoolean isClosed = new AtomicBoolean(false);
    final AtomicBoolean contaminatedConnection = new AtomicBoolean(false);
    private final Gson gson = new Gson();
    private final AtomicBoolean hasUncommittedData = new AtomicBoolean(false);


    private ConsumerRecord currentRecord = null;


    public RabbitMqConsumerConnectionImpl(String factoryName,
                                          String sourceUrl,
                                          RollbackStrategy rollbackStrategy,
                                          DlqService dlqService,
                                          Connection connection,
                                          Channel channel,
                                          String queueName,
                                          GaugeMetric lastReceiveTime) throws IOException {
        this.factoryName = factoryName;
        this.sourceUrl = sourceUrl;
        this.rollbackStrategy = rollbackStrategy;
        this.dlqService = dlqService;


        this.connection = connection;
        this.channel = channel;
        this.lastReceiveTime = lastReceiveTime;

        channel.basicConsume(queueName, this::handleDelivery, this::handleCancel, this::handleShutdownSignal);
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

    private void handleDelivery(String consumerTag, Delivery message) throws IOException {
        lastReceiveTime.set(System.currentTimeMillis());
        buffer.add(new ConsumerRecord(consumerTag, message.getEnvelope(), message.getProperties(), message.getBody()));
    }

    public void handleCancel(String consumerTag) throws IOException {
        contaminatedConnection.set(true);
    }

    public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
        contaminatedConnection.set(true);
    }

    @Override
    public void riseUncommittedData() {
        hasUncommittedData.set(true);
    }

    @Override
    public boolean hasUncommittedData() {

        return hasUncommittedData.get();
    }

    @Override
    public String getName() {
        return factoryName;
    }


    @Override
    public void beginTransaction() {

    }

    @Override
    public void rollbackTransaction() {
        if (currentRecord == null)
            return;
        switch (rollbackStrategy) {
            case RETRY -> {
                try {
                    channel.basicNack(currentRecord.envelope().getDeliveryTag(), false, true);


                } catch (IOException e) {
                    contaminatedConnection.set(true);
                    log.error(LogMessage.create("Error rollback on retry")
                            .addParam("stacktrace", e)
                    );
                    throw new OctopusSourceConnectionException("Error on rollback.", e);
                } finally {
                    hasUncommittedData.set(false);
                    currentRecord = null;
                }
                log.info("Transaction rolled back");

            }
            case DISCARD -> {
                try {

                    channel.basicAck(currentRecord.envelope().getDeliveryTag(), false);

                    log.info("Transaction committed");

                } catch (IOException e) {
                    throw new TransactionManagerException("Unknown exception", e);
                } finally {
                    hasUncommittedData.set(false);
                    currentRecord = null;
                }

                log.debug("Transaction rolled back. Message committed  because of discard strategy");
            }

            case DLQ -> {

                try {
                    DlqMessage dlqMessage = new DlqMessage(currentRecord.consumerTag(), currentRecord.envelope(),
                            currentRecord.properties(), currentRecord.body());

                    UUID id = dlqService.add(sourceUrl, gson.toJson(dlqMessage).getBytes(StandardCharsets.UTF_8), null);
                    log.warn(new LogMessage("Message send to dlq.").addParam("dlqMessageId", id));
                } catch (Exception e1) {

                    log.fatal("Failed put message to DLQ. Downgrading to retry", e1);
                    try {
                        channel.basicNack(currentRecord.envelope().getDeliveryTag(), false, true);

                    } catch (IOException e) {
                        contaminatedConnection.set(true);
                        log.error(LogMessage.create("Error rolling back massage on dlq failed")
                                .addParam("stacktrace", e)
                        );
                        throw new OctopusSourceConnectionException("Error on rollback.", e);
                    } finally {
                        hasUncommittedData.set(false);
                        currentRecord = null;
                    }
                    log.info("Transaction rolled back");

                }
            }
        }

    }

    @Override
    public void commitTransaction() {
        if (currentRecord == null)
            return;

        try {

            channel.basicAck(currentRecord.envelope().getDeliveryTag(), false);

            log.info("Transaction committed");

        } catch (IOException e) {
            throw new TransactionManagerException("Unknown exception", e);
        } finally {
            hasUncommittedData.set(false);
            currentRecord = null;
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
    public ConsumerRecord getRecord() {

        if (currentRecord != null) {
            log.error(LogMessage.create("Connection has uncommitted message")

            );
            throw new OctopusSourceConnectionException("Connection has uncommitted message in processing", null);
        }
        currentRecord = buffer.poll();
        if (currentRecord != null)
            hasUncommittedData.set(true);
        return currentRecord;
    }

    @Override
    public DlqMessage getDlqMessage() {
        if (dlqService != null) {
            try {
                DlqRecord record = dlqService.get(sourceUrl);
                log.info(new LogMessage("Get dlq message ").addParam("dlqMessageId", record.id()));
                DlqMessage dlqMessage = gson.fromJson(new String(record.data(), StandardCharsets.UTF_8),
                        DlqMessage.class);
                dlqMessage.setId(record.id());
                return dlqMessage;
            } catch (Throwable e) {
                log.error(LogMessage.create("Error getting dlq message")
                        .addParam("stacktrace", e)
                );
                throw new OctopusSourceConnectionException("Error getting dlq message", e);
            }
        } else return null;
    }


}
