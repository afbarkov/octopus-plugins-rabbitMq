package ru.integrotechnologies.octopus.library.rabbitmq.connection.consumer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

public record ConsumerRecord(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
}

