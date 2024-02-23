package org.lexna.octopus.library.rabbitmq;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;
import org.lexna.octopus.internal.enums.SerializationType;
import org.lexna.octopus.library.rabbitmq.connection.producer.RabbitMqProducerConnectionFactoryConfig;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "config", namespace = "org/lexna/octopus/plugins/rabbitmq/RabbitMQOutputNode")
public class RabbitMQOutputNodeConfig {
    final static String SETTINGS_NAMESPACE = "org/lexna/octopus/plugins/rabbitmq/RabbitMQOutputNode";
    @XmlElement(namespace = SETTINGS_NAMESPACE, required = true)
    String connectionName;
    @XmlElement(namespace = SETTINGS_NAMESPACE, required = true)
    String exchange;
    @XmlElement(namespace = SETTINGS_NAMESPACE, required = true)
    String routeKey;
    @XmlElement(namespace = SETTINGS_NAMESPACE, defaultValue = "DATA")
    SerializationType serializationType = SerializationType.DATA;


}
