package ru.integrotechnologies.octopus.library.rabbitmq.connection.consumer;

import jakarta.xml.bind.annotation.*;
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "config", namespace = "ru/integrotechnologies/octopus/plugins/rabbitmq/connection/RabbitMqConsumerConnectionFactory")
public class RabbitMqConsumerConnectionFactoryConfig {

    final static String SETTINGS_NAMESPACE = "ru/integrotechnologies/octopus/plugins/rabbitmq/connection/RabbitMqConsumerConnectionFactory";

    @XmlElement(namespace = SETTINGS_NAMESPACE, required = true)
    String host;
    @XmlElement(namespace = SETTINGS_NAMESPACE, required = false)
    String virtualHost;
    @XmlElement(namespace = SETTINGS_NAMESPACE, required = true)
    int port;
    @XmlElement(namespace = SETTINGS_NAMESPACE, required = true)
    RabbitMqConsumerConnectionFactoryConfig.Credentials credentials;
    @XmlElement(namespace = SETTINGS_NAMESPACE, required = true)
    String queueName;
    @XmlElement(namespace = SETTINGS_NAMESPACE, defaultValue = "false")
    boolean useSSL = false;
    @XmlElement(namespace = SETTINGS_NAMESPACE)
    RollbackStrategy rollbackStrategy = RollbackStrategy.RETRY;
    @XmlElement(namespace = SETTINGS_NAMESPACE)
    String dlqService;
    @XmlElement(namespace = SETTINGS_NAMESPACE, required = true)
    Integer maxConnections = 1;
    @XmlElement(namespace = SETTINGS_NAMESPACE, required = true)
    Integer connectionWaitingTimeout = 15;

    @XmlElement(namespace = SETTINGS_NAMESPACE, required = true)
    Integer prefetchCount = 15;
    @XmlElement(namespace = SETTINGS_NAMESPACE, required = true)
    Integer commitPriority = 1;

    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(namespace = SETTINGS_NAMESPACE)
    public static class Credentials {
        @XmlElement(namespace = SETTINGS_NAMESPACE, required = true)
        String login;
        @XmlElement(namespace = SETTINGS_NAMESPACE, required = true)
        String password;

    }
}
