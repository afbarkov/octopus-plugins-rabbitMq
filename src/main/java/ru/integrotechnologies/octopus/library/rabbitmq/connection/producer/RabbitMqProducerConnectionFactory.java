package org.lexna.octopus.library.rabbitmq.connection.producer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.extern.log4j.Log4j2;
import org.lexna.octopus.exceptions.OctopusException;
import org.lexna.octopus.exceptions.OctopusSourceConnectionException;
import org.lexna.octopus.internal.WorkflowContext;
import org.lexna.octopus.internal.config.ConnectionConfig;
import org.lexna.octopus.internal.resources.SourceConnectionFactory;
import org.lexna.octopus.library.rabbitmq.connection.consumer.RabbitMqConsumerConnectionFactoryConfig;
import org.lexna.octopus.loggin.LogMessage;
import org.lexna.octopus.metrics.GaugeMetric;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

@Log4j2
public class RabbitMqProducerConnectionFactory implements SourceConnectionFactory<RabbitMqProducerConnection> {
    private final WorkflowContext workflowContext;
    private final RabbitMqProducerConnectionFactoryConfig config;
    private final List<RabbitMqProducerConnectionImpl> issuedProducers = new ArrayList<>();
    private final Queue<RabbitMqProducerConnectionImpl> freeProducers = new LinkedList<>();
    private final ConnectionFactory rabbitMqFactory;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final Integer commitPriority;
    private final String name;
    private final String metricName;
    private final GaugeMetric lastSendDuration;
    public RabbitMqProducerConnectionFactory(ConnectionConfig config, WorkflowContext workflowContext) {
        this.workflowContext = workflowContext;
        commitPriority=config.getCommitPriority();
        try {
            this.config = workflowContext.loadImplementationConfig(
                    RabbitMqProducerConnectionFactoryConfig.class,
                    getClass().getResource("/xsd/" + getClass().getSimpleName() + ".xsd"),
                    config,
                    RabbitMqProducerConnectionFactoryConfig.SETTINGS_NAMESPACE);
        }
        catch (OctopusException e)
        {
            log.error(LogMessage.create("Error loading config")
                            .addParam("connectionName",config.getName())
                    ,e);
            throw e;
        }
        this.name=config.getName();

        rabbitMqFactory = new ConnectionFactory();
        rabbitMqFactory.setHost(this.config.host);
        rabbitMqFactory.setPort(this.config.port);
        rabbitMqFactory.setRequestedHeartbeat(60);
        if (this.config.virtualHost != null) rabbitMqFactory.setVirtualHost(this.config.virtualHost);

        rabbitMqFactory.setUsername(this.config.credentials.login);
        rabbitMqFactory.setPassword(this.config.credentials.password);
        rabbitMqFactory.setAutomaticRecoveryEnabled(true);
        if (this.config.useSSL) {
            try {
                rabbitMqFactory.useSslProtocol();
            } catch (NoSuchAlgorithmException | KeyManagementException e) {
                log.error(LogMessage.create("No free connection. Waiting for free connection")

                );
                throw new OctopusException("Unknown ssl exception", e);
            }
        }

        if(config.getMetricName()!=null)
        {
            this.metricName=config.getMetricName();
        }
        else
        {
            this.metricName=name;
        }
        lastSendDuration =workflowContext.createGaugeMetric("connectionFactory_"+metricName+"_lastSendDuration",null);
    }

    @Override
    public RabbitMqProducerConnection getNewConnection() {
        if (isClosed.get()) {
            log.error(LogMessage.create("Attempt to access closed connection")

            );
            throw new OctopusSourceConnectionException("Factory is closed", null);
        }
        boolean waited = false;
        do {
            //check if free connection
            synchronized (freeProducers) {
                if (freeProducers.size() > 0) {
                    log.debug(LogMessage.create("Try to lease existing connection")
                            .addParam("connectionType", RabbitMqProducerConnection.class.getCanonicalName())
                            .addParam("connectionName", name)
                    );
                    RabbitMqProducerConnectionImpl consumer = freeProducers.poll();
                    if (!consumer.checkReadiness()) {
                        consumer.contaminatedConnection.set(true);
                        log.debug(LogMessage.create("Broken connection detected")
                                .addParam("connectionType", RabbitMqProducerConnection.class.getCanonicalName())
                                .addParam("connectionName", name)
                        );
                        releaseConnection(consumer);
                        continue;
                    }
                    return new RabbitMqProducerPoolWrapper(consumer, this::releaseConnection);
                }
            }
            //no free connection, is new connection possible
            synchronized (issuedProducers) {
                if (issuedProducers.size() < config.maxConnections) {
                    log.debug(LogMessage.create("No free connection. Creating new one")
                            .addParam("connectionType", RabbitMqProducerConnection.class.getCanonicalName())
                            .addParam("connectionName", name)
                    );
                    RabbitMqProducerConnectionImpl newConnection = createNewConnection();
                    issuedProducers.add(newConnection);
                    return new RabbitMqProducerPoolWrapper(newConnection, this::releaseConnection);
                }
            }
            if (waited) {
                log.error(LogMessage.create("Connection waiting timeout")

                );
                throw new OctopusException("Connection waiting timeout", null);
            }


            synchronized (freeProducers) {
                try {
                    log.debug(LogMessage.create("No free connection. Waiting for free connection")
                            .addParam("connectionType", RabbitMqProducerConnection.class.getCanonicalName())
                            .addParam("connectionName", name)
                    );
                    freeProducers.wait(config.connectionWaitingTimeout);
                    waited = true;
                } catch (InterruptedException e) {
                    log.error(LogMessage.create("Connection waiting aborted")
                            .addParam("stacktrace", e)
                    );
                    throw new OctopusException("Connection waiting aborted", e);
                }
            }
        } while (true);
    }

    @Override
    public int getCommitPriority() {
        return config.commitPriority;
    }


    private RabbitMqProducerConnectionImpl createNewConnection() {
        log.debug(LogMessage.create("Creating new connection")
                .addParam("connectionType", RabbitMqProducerConnection.class.getCanonicalName())
                .addParam("connectionName", name)
        );
        Connection connection;
        try {
            connection = rabbitMqFactory.newConnection();
        } catch (Exception e) {
            log.error(LogMessage.create("Error creating rabbitMq connection")
                    .addParam("stacktrace", e)
            );
            throw new OctopusSourceConnectionException("error creating rabbitMQ connection", e);
        }
        Channel channel;
        try {
            channel = connection.createChannel();

        } catch (Exception e) {
            try {
                connection.close();
            } catch (IOException ignored) {

            }
            log.error(LogMessage.create("Error creating rabbitMq channel")
                    .addParam("stacktrace", e)
            );
            throw new OctopusSourceConnectionException("error creating rabbitMQ channel", e);
        }

        try {
            return new RabbitMqProducerConnectionImpl(name, connection, channel,
                    lastSendDuration);
        } catch (IOException e) {
            log.error(LogMessage.create("Error creating new connection")
                    .addParam("stacktrace", e)
            );
            throw new OctopusSourceConnectionException("Error creating new connection", e);
        }
    }

    private void releaseConnection(RabbitMqProducerConnectionImpl connection) {
        synchronized (issuedProducers) {
            issuedProducers.remove(connection);
            if (connection.contaminatedConnection.get()) {
                try {
                    connection.close();
                } catch (IOException ignored) {

                }
                synchronized (freeProducers) {
                    freeProducers.notify();
                }
            } else {
                issuedProducers.remove(connection);
                synchronized (freeProducers) {
                    freeProducers.add(connection);
                    freeProducers.notify();
                }
            }
        }
        log.debug(LogMessage.create("Connection released")
                .addParam("connectionType", RabbitMqProducerConnection.class.getCanonicalName())
                .addParam("connectionName", name)
        );

    }

    @Override
    public void close() throws Exception {
        isClosed.set(true);
        synchronized (freeProducers) {
            for (RabbitMqProducerConnectionImpl c : freeProducers) {
                c.close();
            }
        }

        synchronized (issuedProducers) {
            for (RabbitMqProducerConnectionImpl c : issuedProducers) {
                c.close();
            }
        }
    }
}
