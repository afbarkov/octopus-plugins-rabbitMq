package ru.integrotechnologies.octopus.library.rabbitmq;

import com.google.gson.Gson;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import ru.integrotechnologies.octopus.exceptions.OctopusNodeProcessionException;
import ru.integrotechnologies.octopus.internal.MessageNode;
import ru.integrotechnologies.octopus.internal.MessageWritableNode;
import ru.integrotechnologies.octopus.internal.WorkflowContext;
import ru.integrotechnologies.octopus.internal.config.NodeConfig;
import ru.integrotechnologies.octopus.internal.templates.AbstractPolingWorkflowInNode;
import ru.integrotechnologies.octopus.internal.templates.PolingResult;
import ru.integrotechnologies.octopus.library.rabbitmq.connection.consumer.ConsumerRecord;
import ru.integrotechnologies.octopus.library.rabbitmq.connection.consumer.DlqMessage;
import ru.integrotechnologies.octopus.library.rabbitmq.connection.consumer.RabbitMqConsumerConnection;
import ru.integrotechnologies.octopus.library.rabbitmq.connection.consumer.RabbitMqConsumerConnectionFactory;
import ru.integrotechnologies.octopus.loggin.LogMessage;
import ru.integrotechnologies.octopus.metrics.GaugeMetric;

import java.util.*;
import java.util.stream.Collectors;

@Log4j2
public class RabbitMQInputNode extends AbstractPolingWorkflowInNode {
    private final RabbitMQInputNodeConfig config;
    private final GaugeMetric lastDeserializationDuration;



    public RabbitMQInputNode(NodeConfig config, WorkflowContext workflowContext) {
        super(config, workflowContext);
        this.config = workflowContext.loadImplementationConfig(RabbitMQInputNodeConfig.class,
                getClass().getResource("/xsd/" + getClass().getSimpleName() + ".xsd"), config,
                RabbitMQInputNodeConfig.SETTINGS_NAMESPACE);
        lastDeserializationDuration =workflowContext.createGaugeMetric("node_"+getMetricName()+"_lastDeserializationDuration",null);
    }

    @Override
    protected PolingResult processSingleIteration() {
        try (RabbitMqConsumerConnection connection = workflowContext.getConnection(
                RabbitMqConsumerConnection.class,
                config.connectionName)) {

            if (config.dlqMessageFirst) {
                DlqMessage dlqMessage = connection.getDlqMessage();
                if (dlqMessage != null) {
                    processDlqMessage(dlqMessage);
                    return PolingResult.SUCCESS;
                } else {
                    ConsumerRecord record = connection.getRecord();
                    if (record != null) {
                        processSingleMessage(record.consumerTag(),
                                record.envelope(),
                                record.properties(),
                                record.body());
                        return PolingResult.SUCCESS;
                    } else {

                        return PolingResult.NO_MESSAGES;
                    }
                }
            } else {
                ConsumerRecord record = connection.getRecord();
                if (record != null) {
                    processSingleMessage(record.consumerTag(), record.envelope(), record.properties(), record.body());
                    return PolingResult.SUCCESS;
                } else {
                    DlqMessage dlqMessage = connection.getDlqMessage();
                    if (dlqMessage != null) {
                        processDlqMessage(dlqMessage);
                        return PolingResult.SUCCESS;
                    }

                    return PolingResult.NO_MESSAGES;
                }

            }
        } catch (Exception exception) {
            log.error(LogMessage.create("Error in pooling loop")
                    .addParam("stacktrace", exception)
            );
            throw new OctopusNodeProcessionException(this, "Unknown exception", exception, null);
        }
    }

    
    @Override
    public void init() {


        log.info("Node initialized");
    }

    @Override
    public void start() {
        super.start();
        log.info("Node started");
    }

    private synchronized void processSingleMessage(String consumerTag,
                                                   Envelope envelope,
                                                   AMQP.BasicProperties properties,
                                                   byte[] body) {
        String processingCorrelationId = UUID.randomUUID().toString();
        ThreadContext.put("processingCorrelationId", processingCorrelationId);
        MessageNode payload = null;
        long t1=System.currentTimeMillis();
        payload = workflowContext.deSerialize(body, config.serializationType);
        lastDeserializationDuration.set(System.currentTimeMillis()-t1);
        getOutputPort("out").propagate(workflowContext.createMessage(payload,
                getMetaData(envelope, properties).lock(),
                null));

        workflowContext.dispatch(this, processingCorrelationId);


//        if (auditInputMessageConverter != null)
//            auditInputMessageConverter.newMessage(envelope, properties, body);


    }


    protected void processDlqMessage(DlqMessage dlqMessage) {

        try {

            log.info(new LogMessage("Processing dlq message ").addParam("dlqMessageId", dlqMessage.getId()));

            processSingleMessage(dlqMessage.getConsumerTag(), dlqMessage.getEnvelope(), dlqMessage.getProperties(),
                    dlqMessage.getPayload());
            log.info(new LogMessage("Dlq message processed ").addParam("dlqMessageId", dlqMessage.getId()));
        } catch (Throwable e) {
            log.warn(new LogMessage("Dlq message processing failed ").addParam("dlqMessageId", dlqMessage.getId()));
//            if (auditInputMessageConverter != null)
//                auditInputMessageConverter.rollbackMessage(dlqMessage.getEnvelope(), dlqMessage.getProperties(),
//                        dlqMessage.getPayload(), e);
            throw e;
        }

    }


    @Override
    public void stop() {
        log.info("Node stopped");
    }

    private MessageWritableNode getMetaData(Envelope envelope, AMQP.BasicProperties properties) {
        List<MessageWritableNode> nodes = new ArrayList<>();
        nodes.add(workflowContext.createMessageNode("exchange", envelope.getExchange()));
        nodes.add(workflowContext.createMessageNode("routingKey", envelope.getRoutingKey()));
        nodes.add(workflowContext.createMessageNode("reDelver", envelope.isRedeliver()));

        if (properties.getCorrelationId() != null)
            nodes.add(workflowContext.createMessageNode("correlationId", properties.getCorrelationId()));
        if (properties.getMessageId() != null)
            nodes.add(workflowContext.createMessageNode("messageId", properties.getMessageId()));
        if (properties.getContentType() != null)
            nodes.add(workflowContext.createMessageNode("contentType", properties.getContentType()));
        if (properties.getContentEncoding() != null)
            nodes.add(workflowContext.createMessageNode("contentEncoding", properties.getContentEncoding()));
        if (properties.getReplyTo() != null)
            nodes.add(workflowContext.createMessageNode("replyTo", properties.getReplyTo()));
        if (properties.getExpiration() != null)
            nodes.add(workflowContext.createMessageNode("expiry", properties.getExpiration()));
        if (properties.getTimestamp() != null)
            nodes.add(workflowContext.createMessageNode("timestamp", properties.getTimestamp().toInstant()));
        if (properties.getHeaders() != null) {
            List<MessageWritableNode> headers = new ArrayList<>();
            for (Map.Entry<String, Object> e : properties.getHeaders().entrySet()) {
                if (e.getValue() instanceof byte[])
                    headers.add(workflowContext.createMessageNode(e.getKey(), (byte[]) e.getValue()));
                else headers.add(workflowContext.createMessageNode(e.getKey(), e.getValue().toString()));
            }
            nodes.add(workflowContext.createMessageNode("headers", headers.stream().map(MessageWritableNode::lock).collect(
                    Collectors.toList())));
        }

        return workflowContext.createMessageNode(null,
                Collections.singletonList(workflowContext.createMessageNode("rabbitMq", nodes.stream().map(MessageWritableNode::lock).collect(
                        Collectors.toList())).lock()));
    }
}
