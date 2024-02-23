package org.lexna.octopus.library.rabbitmq.connection.consumer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import lombok.Getter;
import lombok.Setter;

import java.time.Instant;
import java.util.*;

public class DlqMessage {

    public DlqMessage(String consumerTag,
                      Envelope envelope,
                      AMQP.BasicProperties properties,
                      byte[] body) {

        this.consumerTag = consumerTag;
        this.deliveryTag = envelope.getDeliveryTag();
        this.redeliver = envelope.isRedeliver();
        exchange = envelope.getExchange();
        routingKey = envelope.getRoutingKey();

        Properties lproperties = new Properties();
        lproperties.contentType = properties.getContentType();
        lproperties.appId = properties.getAppId();
        lproperties.contentEncoding = properties.getContentEncoding();
        lproperties.deliveryMode = properties.getDeliveryMode();
        lproperties.priority = properties.getPriority();
        lproperties.correlationId = properties.getCorrelationId();
        lproperties.replyTo = properties.getReplyTo();
        lproperties.expiration = properties.getExpiration();
        lproperties.messageId = properties.getMessageId();
        lproperties.timestamp = Optional.ofNullable(properties.getTimestamp()).map(Date::toInstant).map(Instant::toEpochMilli).orElse(
                null);
        lproperties.type = properties.getType();
        lproperties.userId = properties.getUserId();
        lproperties.appId = properties.getAppId();
        lproperties.clusterId = properties.getClusterId();
        if (properties.getHeaders() != null) {
            lproperties.headers = new HashMap<>();
            for (Map.Entry<String, Object> e : properties.getHeaders().entrySet()) {
                lproperties.headers.put(e.getKey(), e.getValue().toString());
            }
        }
        payload = Optional.ofNullable(body).map(x -> Base64.getEncoder().encodeToString(x)).orElse(null);


    }

    @Getter
    @Setter
    private transient UUID id;
    private String payload;
    @Getter
    private final String consumerTag;
    private final long deliveryTag;
    private final boolean redeliver;
    private final String exchange;
    private final String routingKey;
    private Properties properties;
    private Map<String, String> headers;

    @Getter
    @Setter
    public class Properties {
        private String contentType;
        private String contentEncoding;
        private Map<String, Object> headers;
        private Integer deliveryMode;
        private Integer priority;
        private String correlationId;
        private String replyTo;
        private String expiration;
        private String messageId;
        private Long timestamp;
        private String type;
        private String userId;
        private String appId;
        private String clusterId;
    }

    public Envelope getEnvelope() {
        return new Envelope(deliveryTag, redeliver, exchange, routingKey);
    }

    public AMQP.BasicProperties getProperties() {

        return new AMQP.BasicProperties(properties.contentType,
                properties.contentEncoding,
                properties.headers,
                properties.deliveryMode,
                properties.priority,
                properties.correlationId,
                properties.getReplyTo(),
                properties.expiration,
                properties.messageId,
                properties.timestamp != null ? Date.from(Instant.ofEpochMilli(properties.timestamp)) : null,
                properties.type,
                properties.userId,
                properties.appId,
                properties.clusterId);
    }

    public byte[] getPayload() {
        return Optional.ofNullable(payload).map(x -> Base64.getDecoder().decode(payload)).orElse(null);
    }
}


