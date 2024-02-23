package org.lexna.octopus.library.rabbitmq;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;
import org.lexna.octopus.internal.enums.SerializationType;
import org.lexna.octopus.library.rabbitmq.connection.consumer.RabbitMqConsumerConnectionFactoryConfig;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "config", namespace = "org/lexna/octopus/plugins/rabbitmq/RabbitMQInputNode")
public class RabbitMQInputNodeConfig {
    final static String SETTINGS_NAMESPACE = "org/lexna/octopus/plugins/rabbitmq/RabbitMQInputNode";
    @XmlElement(namespace = SETTINGS_NAMESPACE)
    String connectionName;

    @XmlElement(namespace = SETTINGS_NAMESPACE)
    SerializationType serializationType = SerializationType.DATA;

    @XmlElement(namespace = SETTINGS_NAMESPACE)
    boolean dlqMessageFirst = false;


}
