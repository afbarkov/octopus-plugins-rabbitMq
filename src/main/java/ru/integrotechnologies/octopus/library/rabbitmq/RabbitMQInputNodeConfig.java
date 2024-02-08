package ru.integrotechnologies.octopus.library.rabbitmq;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;
import ru.integrotechnologies.octopus.internal.enums.SerializationType;
import ru.integrotechnologies.octopus.library.rabbitmq.connection.consumer.RabbitMqConsumerConnectionFactoryConfig;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "config", namespace = "ru/integrotechnologies/octopus/plugins/rabbitmq/RabbitMQInputNode")
public class RabbitMQInputNodeConfig {
    final static String SETTINGS_NAMESPACE = "ru/integrotechnologies/octopus/plugins/rabbitmq/RabbitMQInputNode";
    @XmlElement(namespace = SETTINGS_NAMESPACE)
    String connectionName;

    @XmlElement(namespace = SETTINGS_NAMESPACE)
    SerializationType serializationType = SerializationType.DATA;

    @XmlElement(namespace = SETTINGS_NAMESPACE)
    boolean dlqMessageFirst = false;


}
