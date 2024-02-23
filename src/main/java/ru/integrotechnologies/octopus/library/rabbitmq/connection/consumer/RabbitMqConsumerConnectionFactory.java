package org.lexna.octopus.library.rabbitmq.connection.consumer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.extern.log4j.Log4j2;
import org.lexna.octopus.common.service.DlqService;
import org.lexna.octopus.exceptions.OctopusException;
import org.lexna.octopus.exceptions.OctopusSourceConnectionException;
import org.lexna.octopus.internal.WorkflowContext;
import org.lexna.octopus.internal.config.ConnectionConfig;
import org.lexna.octopus.internal.resources.SourceConnectionFactory;
import org.lexna.octopus.loggin.LogMessage;
import org.lexna.octopus.metrics.GaugeMetric;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

@Log4j2
public class RabbitMqConsumerConnectionFactory implements SourceConnectionFactory<RabbitMqConsumerConnection> {
    private final WorkflowContext workflowContext;
    private final RabbitMqConsumerConnectionFactoryConfig config;
    private final List<RabbitMqConsumerConnectionImpl> issuedConsumers = new ArrayList<>();
    private final Queue<RabbitMqConsumerConnectionImpl> freeConsumers = new LinkedList<>();
    private final DlqService dlqService;
    private final ConnectionFactory rabbitMqFactory;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final Integer commitPriority;
    private final String name;
    private final String metricName;

    private final GaugeMetric lastReceiveTime;

    public RabbitMqConsumerConnectionFactory(ConnectionConfig config, WorkflowContext workflowContext) {
        this.workflowContext = workflowContext;
        commitPriority=config.getCommitPriority();
        try {
            this.config = workflowContext.loadImplementationConfig(
                    RabbitMqConsumerConnectionFactoryConfig.class,
                    getClass().getResource("/xsd/" + getClass().getSimpleName() + ".xsd"),
                    config,
                    RabbitMqConsumerConnectionFactoryConfig.SETTINGS_NAMESPACE);
        }
        catch (OctopusException e)
        {
            log.error(LogMessage.create("Error loading config")
                            .addParam("connectionName",config.getName())
                    ,e);
            throw e;
        }
        this.name=config.getName();
        if (this.config.dlqService != null)
            dlqService = (DlqService) workflowContext.getService(this.config.dlqService);
        else
            dlqService = null;

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
                log.error(LogMessage.create("Error enabling ssl")
                        .addParam("stacktrace", e)
                );
                throw new OctopusException("Unknown exception", e);
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

        lastReceiveTime =workflowContext.createGaugeMetric("connectionFactory_"+metricName+"_lastReceiveTime",null);
    }

    @Override
    public RabbitMqConsumerConnection getNewConnection() {
        if (isClosed.get()) {
            log.error(LogMessage.create("Atte,pt to access closed factory"));
            throw new OctopusSourceConnectionException("Factory is closed", null);
        }
        boolean waited = false;
        do {
            //check if free connection
            synchronized (freeConsumers) {
                if (freeConsumers.size() > 0) {
                    log.debug(LogMessage.create("Try to lease existing connection")
                            .addParam("connectionType", RabbitMqConsumerConnection.class.getCanonicalName())
                            .addParam("connectionName", name)
                    );
                    RabbitMqConsumerConnectionImpl consumer = freeConsumers.poll();
                    if (!consumer.checkReadiness()) {
                        log.debug(LogMessage.create("Broken connection detected")
                                .addParam("connectionType", RabbitMqConsumerConnection.class.getCanonicalName())
                                .addParam("connectionName", name)
                        );

                        consumer.contaminatedConnection.set(true);
                        releaseConnection(consumer);
                        continue;
                    }
                    return new RabbitMqConsumerPoolWrapper(consumer, this::releaseConnection);
                }
            }
            //no free connection, is new connection possible
            synchronized (issuedConsumers) {
                if (issuedConsumers.size() < config.maxConnections) {
                    log.debug(LogMessage.create("No free connection. Creating new one")
                            .addParam("connectionType", RabbitMqConsumerConnection.class.getCanonicalName())
                            .addParam("connectionName", name)
                    );
                    RabbitMqConsumerConnectionImpl newConnection = createNewConnection();
                    issuedConsumers.add(newConnection);
                    return new RabbitMqConsumerPoolWrapper(newConnection, this::releaseConnection);
                }
            }
            if (waited) {
                log.error(LogMessage.create("Connection waiting timeout"));
                throw new OctopusException("Connection waiting timeout", null);
            }


            synchronized (freeConsumers) {
                try {
                    log.debug(LogMessage.create("No free connection. Waiting for free connection")
                            .addParam("connectionType", RabbitMqConsumerConnection.class.getCanonicalName())
                            .addParam("connectionName", name)
                    );
                    freeConsumers.wait(config.connectionWaitingTimeout);
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


    private RabbitMqConsumerConnectionImpl createNewConnection() {
        log.debug(LogMessage.create("Creating new connection")
                .addParam("connectionType", RabbitMqConsumerConnection.class.getCanonicalName())
                .addParam("connectionName", name)
        );
        String uriVirtualhost = (this.config.virtualHost == null || this.config.virtualHost.trim()
                .equals("/")) ? "" : this.config.virtualHost;
        URI sourceUri;
        try {
            sourceUri = new URI("rabbitmq", null, this.config.host, this.config.port,
                    uriVirtualhost + "/" + this.config.queueName, null, null);
        } catch (URISyntaxException e) {
            log.error(LogMessage.create("Error creating source uri")
                    .addParam("stacktrace", e)
            );
            throw new OctopusException("Uri creation exception", e);
        }
        Connection connection;
        try {
            connection = rabbitMqFactory.newConnection();
        } catch (Exception e) {
            log.error(LogMessage.create("Error creating rabbitmq connection")
                    .addParam("stacktrace", e)
            );
            throw new OctopusSourceConnectionException("error creating rabbitMQ connection", e);
        }
        Channel channel;
        try {
            channel = connection.createChannel();
            channel.basicQos(config.prefetchCount);

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
            return new RabbitMqConsumerConnectionImpl(name,
                    sourceUri.toString(),
                    config.rollbackStrategy,
                    dlqService,
                    connection,
                    channel,
                    config.queueName, lastReceiveTime);
        } catch (IOException e) {
            log.error(LogMessage.create("Error creating new connection")
                    .addParam("stacktrace", e)
            );
            throw new OctopusSourceConnectionException("Error creating new connection", e);
        }
    }

    private void releaseConnection(RabbitMqConsumerConnectionImpl connection) {
        synchronized (issuedConsumers) {
            issuedConsumers.remove(connection);
            if (connection.contaminatedConnection.get()) {
                try {
                    connection.close();
                } catch (IOException ignored) {

                }
                synchronized (freeConsumers) {
                    freeConsumers.notify();
                }
            } else {
                issuedConsumers.remove(connection);
                synchronized (freeConsumers) {
                    freeConsumers.add(connection);
                    freeConsumers.notify();
                }
            }
        }
        log.debug(LogMessage.create("Connection released")
                .addParam("connectionType", RabbitMqConsumerConnection.class.getCanonicalName())
                .addParam("connectionName", name)
        );
    }

    @Override
    public void close() throws Exception {
        isClosed.set(true);
        synchronized (freeConsumers) {
            for (RabbitMqConsumerConnectionImpl c : freeConsumers) {
                c.close();
            }
        }

        synchronized (issuedConsumers) {
            for (RabbitMqConsumerConnectionImpl c : issuedConsumers) {
                c.close();
            }
        }
    }
}
