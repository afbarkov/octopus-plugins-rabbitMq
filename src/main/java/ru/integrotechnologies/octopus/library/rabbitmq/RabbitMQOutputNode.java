package ru.integrotechnologies.octopus.library.rabbitmq;

import com.rabbitmq.client.AMQP;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.Logger;
import ru.integrotechnologies.octopus.exceptions.OctopusNodeProcessionException;
import ru.integrotechnologies.octopus.internal.Message;
import ru.integrotechnologies.octopus.internal.MessageNode;
import ru.integrotechnologies.octopus.internal.WorkflowContext;
import ru.integrotechnologies.octopus.internal.WorkflowExecutionContext;
import ru.integrotechnologies.octopus.internal.config.NodeConfig;
import ru.integrotechnologies.octopus.internal.templates.AbstractInOutNode;
import ru.integrotechnologies.octopus.library.rabbitmq.connection.producer.RabbitMqProducerConnection;
import ru.integrotechnologies.octopus.library.rabbitmq.connection.producer.RabbitMqProducerConnectionFactory;
import ru.integrotechnologies.octopus.loggin.LogMessage;
import ru.integrotechnologies.octopus.metrics.GaugeMetric;

import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

@Log4j2
public class RabbitMQOutputNode extends AbstractInOutNode {
    private final RabbitMQOutputNodeConfig config;
    private final GaugeMetric lastProcessingDurationMetric;
    private final GaugeMetric lastSerializationDuration;
    public RabbitMQOutputNode(NodeConfig config, WorkflowContext workflowContext) {
        super(config, workflowContext);
        this.config = workflowContext.loadImplementationConfig(RabbitMQOutputNodeConfig.class,
                getClass().getResource("/xsd/" + getClass().getSimpleName() + ".xsd"),
                config,
                RabbitMQOutputNodeConfig.SETTINGS_NAMESPACE);
        lastProcessingDurationMetric=workflowContext.createGaugeMetric("node_"+getMetricName()+"_lastProcessingDuration",null);
        lastSerializationDuration=workflowContext.createGaugeMetric("node_"+getMetricName()+"_lastSerializationDuration",null);
    }

    
    @Override
    public void init() {
        log.info("Node initialized");
    }

    @Override
    public void start() {
        log.info("Node started");

    }

    @Override
    public void stop() {

        log.info("Node stopped");
    }


    @Override
    protected void processInputMessage(String portName,
                                       Message message,
                                       WorkflowExecutionContext workflowExecutionContext) {
        long t1=System.currentTimeMillis();
        try (RabbitMqProducerConnection connection = workflowContext.getConnection(
                RabbitMqProducerConnection.class,
                config.connectionName)) {
            AMQP.BasicProperties.Builder bp = new AMQP.BasicProperties.Builder();
            log.info("Begin message processing");
            bp.deliveryMode(2);
            bp.contentType("application/octet-stream");
            if (message.getPayloadMetaRoot() != null)
                setMetaData(bp, message.getPayloadMetaRoot());
            long t2=System.currentTimeMillis();
            byte[] body = workflowContext.serialize(message.getPayloadRoot(), config.serializationType);
            lastSerializationDuration.set(System.currentTimeMillis()-t2);
            connection.post(config.exchange, config.routeKey,bp.build(), body);
        } catch (Exception e) {
            log.error(LogMessage.create("Error processing message")
                    .addParam("stacktrace", e)
            );
            throw new OctopusNodeProcessionException(this,
                    "Processing message",
                    e,
                    workflowExecutionContext.getProcessingCorrelationId());
        }
        if (getOutputPort("out") != null)
            getOutputPort("out").propagate(message);
        lastProcessingDurationMetric.set(System.currentTimeMillis()-t1);

    }


    private void setMetaData(AMQP.BasicProperties.Builder properties, MessageNode metaNode) {
        if (metaNode.getChild("rabbitMq") != null) {
            MessageNode metaRoot = metaNode.getChild("rabbitMq");
            if (metaRoot.getChild("correlationId") != null)
                properties.correlationId(metaRoot.getChild("correlationId").getValueAsString());
            if (metaRoot.getChild("messageId") != null)
                properties.messageId(metaRoot.getChild("messageId").getValueAsString());
            if (metaRoot.getChild("contentType") != null)
                properties.contentType(metaRoot.getChild("contentType").getValueAsString());
            if (metaRoot.getChild("contentEncoding") != null)
                properties.contentEncoding(metaRoot.getChild("contentEncoding").getValueAsString());
            if (metaRoot.getChild("replyTo") != null)
                properties.replyTo(metaRoot.getChild("replyTo").getValueAsString());
            if (metaRoot.getChild("expiry") != null)
                properties.expiration(metaRoot.getChild("expiry").getValueAsString());
            if (metaRoot.getChild("timestamp") != null) {
                if(metaRoot.getChild("timestamp").getValue() instanceof Instant)
                    properties.timestamp(Date.from((Instant) metaRoot.getChild("timestamp").getValue()));
            }
            if (metaRoot.getChild("headers") != null) {
                Map<String, Object> mHeaders = new HashMap<>();
                for (Iterator<MessageNode> it = metaRoot.getChild("headers").childrenIterator(); it.hasNext(); ) {
                    MessageNode h = it.next();
                    if (h.getValue() instanceof byte[])
                        mHeaders.put(h.getName(), h.getValueBytes());
                    else
                        mHeaders.put(h.getName(), h.getValueAsString());
                }
                properties.headers(mHeaders);
            }
        }
    }
}
