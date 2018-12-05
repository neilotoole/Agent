/*******************************************************************************
 * Copyright (c) 2018 Edgeworx, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License 2.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v20.html
 *
 * Contributors:
 * Saeid Baghbidi
 * Kilton Hopkins
 *  Ashita Nagar
 *******************************************************************************/
package org.eclipse.iofog.message_bus;

import org.eclipse.iofog.IOFogModule;
import org.eclipse.iofog.connector_client.ConnectorManager;
import org.eclipse.iofog.microservice.Microservice;
import org.eclipse.iofog.microservice.MicroserviceManager;
import org.eclipse.iofog.microservice.Receiver;
import org.eclipse.iofog.microservice.Route;
import org.eclipse.iofog.status_reporter.StatusReporter;
import org.eclipse.iofog.utils.configuration.Configuration;
import org.eclipse.iofog.utils.logging.LoggingService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;
import static org.eclipse.iofog.utils.Constants.MESSAGE_BUS;
import static org.eclipse.iofog.utils.Constants.ModulesStatus.STOPPED;

/**
 * Message Bus module
 *
 * @author saeid
 */
public class MessageBus implements IOFogModule {

    final static String MODULE_NAME = "Message Bus";

    private MessageBusServer messageBusServer;
    private Map<String, Route> routes;
    private Map<String, MessagePublisher> publishers;
    private Map<String, MessageReceiver> receivers;
    private MessageIdGenerator idGenerator;
    private static MessageBus instance;
    private MicroserviceManager microserviceManager;
    private final Object updateLock = new Object();

    private long lastSpeedTime, lastSpeedMessageCount;

    private MessageBus() {
    }

    @Override
    public int getModuleIndex() {
        return MESSAGE_BUS;
    }

    @Override
    public String getModuleName() {
        return MODULE_NAME;
    }

    public static MessageBus getInstance() {
        if (instance == null) {
            synchronized (MessageBus.class) {
                if (instance == null) {
                    instance = new MessageBus();
                }
            }
        }
        return instance;
    }


    /**
     * enables real-time {@link Message} receiving of an {@link Microservice}
     *
     * @param receiver - ID of {@link Microservice}
     */
    public synchronized void enableRealTimeReceiving(String receiver) {
        MessageReceiver rec = receivers.get(receiver);
        if (rec == null)
            return;
        rec.enableRealTimeReceiving();
    }

    /**
     * disables real-time {@link Message} receiving of an {@link Microservice}
     *
     * @param receiver - ID of {@link Microservice}
     */
    public synchronized void disableRealTimeReceiving(String receiver) {
        MessageReceiver rec = receivers.get(receiver);
        if (rec == null)
            return;
        rec.disableRealTimeReceiving();
    }

    /**
     * initialize list of {@link Message} publishers and receivers
     */
    private void init() {
        lastSpeedMessageCount = 0;
        lastSpeedTime = System.currentTimeMillis();

        routes = microserviceManager.getRoutes();
        idGenerator = new MessageIdGenerator();
        publishers = new ConcurrentHashMap<>();
        receivers = new ConcurrentHashMap<>();

        if (routes == null)
            return;

        routes.entrySet().stream()
            .filter(route -> route.getValue() != null)
            .filter(route -> route.getValue().getReceivers() != null)
            .forEach(entry -> {
                String publisher = entry.getKey();
                Route route = entry.getValue();

                try {
                    messageBusServer.createProducer(publisher);
                } catch (Exception e) {
                    LoggingService.logWarning(MODULE_NAME + "(" + publisher + ")",
                        "unable to start publisher module --> " + e.getMessage());
                }
                publishers.put(publisher, new MessagePublisher(route, messageBusServer.getProducer(publisher)
                ));

                receivers.putAll(entry.getValue().getReceivers()
                    .stream()
                    .filter(item -> !receivers.containsKey(item.getMicroserviceUuid()))
                    .collect(toMap(Receiver::getMicroserviceUuid, item -> {
                        try {
                            messageBusServer.createConsumer(item.getMicroserviceUuid());
                        } catch (Exception e) {
                            LoggingService.logWarning(MODULE_NAME + "(" + item + ")",
                                "unable to start receiver module --> " + e.getMessage());
                        }
                        return new MessageReceiver(
                            item.getMicroserviceUuid(),
                            item.isLocal(),
                            item.getConnectorProducerConfig(),
                            messageBusServer.getConsumer(item.getMicroserviceUuid())
                        );
                    })));
            });

    }

    /**
     * calculates the average speed of {@link Message} moving through ioFog
     */
    private final Runnable calculateSpeed = () -> {
        while (true) {
            try {
                Thread.sleep(Configuration.getSpeedCalculationFreqMinutes() * 60 * 1000);

                logInfo("calculating message processing speed");

                long now = System.currentTimeMillis();
                long msgs = StatusReporter.getMessageBusStatus().getProcessedMessages();

                float speed = ((float) (msgs - lastSpeedMessageCount)) / ((now - lastSpeedTime) / 1000f);
                StatusReporter.setMessageBusStatus().setAverageSpeed(speed);
                lastSpeedMessageCount = msgs;
                lastSpeedTime = now;
            } catch (Exception exp) {
                logWarning(exp.getMessage());
            }
        }
    };

    /**
     * monitors ActiveMQ server
     */
    private final Runnable checkMessageServerStatus = () -> {
        while (true) {
            try {
                Thread.sleep(5000);

                logInfo("check message bus server status");
                if (!messageBusServer.isServerActive()) {
                    logWarning("server is not active. restarting...");
                    stop();
                    try {
                        messageBusServer.startServer();
                        logInfo("server restarted");
                        init();
                    } catch (Exception e) {
                        logWarning("server restart failed --> " + e.getMessage());
                    }
                }

                publishers.forEach((publisher, value) -> {
                    if (messageBusServer.isProducerClosed(publisher)) {
                        logWarning("producer module for " + publisher + " stopped. restarting...");
                        value.close();
                        Route route = routes.get(publisher);
                        if (route == null || route.getReceivers() == null || route.getReceivers().size() == 0) {
                            publishers.remove(publisher);
                        } else {
                            try {
                                messageBusServer.createProducer(publisher);
                                publishers.put(publisher, new MessagePublisher(
                                    route,
                                    messageBusServer.getProducer(publisher)));
                                logInfo("producer module restarted");
                            } catch (Exception e) {
                                logWarning("unable to restart producer module for " + publisher + " --> " + e.getMessage());
                            }
                        }
                    }
                });

                receivers.forEach((receiver, value) -> {
                    if (messageBusServer.isConsumerClosed(receiver)) {
                        logWarning("consumer module for " + receiver + " stopped. restarting...");
                        value.close();
                        try {
                            messageBusServer.createConsumer(receiver);
                            receivers.put(receiver, new MessageReceiver(
                                receiver,
                                value.isLocal(),
                                value.getConnectorProducerConfig(),
                                messageBusServer.getConsumer(receiver)));
                            logInfo("consumer module restarted");
                        } catch (Exception e) {
                            logWarning("unable to restart consumer module for " + receiver + " --> " + e.getMessage());
                        }
                    }
                });
            } catch (Exception exp) {
                logWarning(exp.getMessage());
            }
        }
    };

    /**
     * updates routing, list of publishers and receivers
     * Field Agent calls this method when any changes applied
     */
    public void update() {
        synchronized (updateLock) {
            ConnectorManager connectorManager = ConnectorManager.INSTANCE;
            Map<String, Route> newRoutes = microserviceManager.getRoutes();

            Map<String, Receiver> newReceivers = newRoutes.values().stream()
                .flatMap(route -> route.getReceivers().stream())
                .collect(toMap(Receiver::getMicroserviceUuid, Function.identity()));

            publishers.forEach((key, value) -> {
                if (!newRoutes.containsKey(key)) {
                    value.close();
                    messageBusServer.removeProducer(key);
                } else {
                    value.updateRoute(newRoutes.get(key));
                }
            });
            publishers.entrySet().removeIf(entry -> !newRoutes.containsKey(entry.getKey()));
            connectorManager.getConnectorConsumers().entrySet().removeIf(entry -> !newRoutes.containsKey(entry.getKey()));
            publishers.putAll(
                newRoutes.values().stream()
                    .filter(route -> !publishers.containsKey(route.getProducer().getMicroserviceId()))
                    .collect(toMap(publisher -> publisher.getProducer().getMicroserviceId(),
                        route -> new MessagePublisher(
                            route,
                            messageBusServer.getProducer(route.getProducer().getMicroserviceId())
                        ))));

            receivers.forEach((key, value) -> {
                if (!newReceivers.containsKey(key)) {
                    value.close();
                    messageBusServer.removeConsumer(key);
                }
            });
            receivers.entrySet().removeIf(entry -> !newReceivers.containsKey(entry.getKey()));
            connectorManager.getConnectorProducers().entrySet().removeIf(entry -> !newReceivers.containsKey(entry.getKey()));

            receivers.putAll(
                newReceivers.values().stream()
                    .filter(receiver -> !receivers.containsKey(receiver.getMicroserviceUuid()))
                    .collect(toMap(Receiver::getMicroserviceUuid,
                        receiver -> new MessageReceiver(
                            receiver.getMicroserviceUuid(),
                            receiver.isLocal(),
                            receiver.getConnectorProducerConfig(),
                            messageBusServer.getConsumer(receiver.getMicroserviceUuid())
                        )
                    )));

            routes = newRoutes;

            List<Microservice> latestMicroservices = microserviceManager.getLatestMicroservices();
            Map<String, Long> publishedMessagesPerMicroservice = StatusReporter.getMessageBusStatus().getPublishedMessagesPerMicroservice();
            publishedMessagesPerMicroservice.keySet().removeIf(key -> !microserviceManager.microserviceExists(latestMicroservices, key));
            latestMicroservices.forEach(e -> {
                if (!publishedMessagesPerMicroservice.keySet().contains(e.getMicroserviceUuid())) {
                    publishedMessagesPerMicroservice.put(e.getMicroserviceUuid(), 0L);
                }
            });
        }
    }

    /**
     * sets  memory usage limit of ActiveMQ
     * {@link Configuration} calls this method when any changes applied
     */
    public void instanceConfigUpdated() {
        messageBusServer.setMemoryLimit();
    }

    /**
     * starts Message Bus module
     */
    public void start() {
        microserviceManager = MicroserviceManager.getInstance();

        messageBusServer = new MessageBusServer();
        try {
            logInfo("STARTING MESSAGE BUS SERVER");
            messageBusServer.startServer();
            messageBusServer.initialize();
        } catch (Exception e) {
            try {
                messageBusServer.stopServer();
            } catch (Exception exp) {
                logWarning(exp.getMessage());
            }
            logWarning("unable to start message bus server --> " + e.getMessage());
            StatusReporter.setSupervisorStatus().setModuleStatus(MESSAGE_BUS, STOPPED);
        }

        logInfo("MESSAGE BUS SERVER STARTED");
        init();

        new Thread(calculateSpeed, "MessageBus : CalculateSpeed").start();
        new Thread(checkMessageServerStatus, "MessageBus : CheckMessageBusServerStatus").start();
    }

    /**
     * closes receivers and publishers and stops ActiveMQ server
     */
    public void stop() {
        for (MessageReceiver receiver : receivers.values())
            receiver.close();

        for (MessagePublisher publisher : publishers.values())
            publisher.close();
        try {
            messageBusServer.stopServer();
        } catch (Exception exp) {
            logWarning(exp.getMessage());
        }
    }

    /**
     * returns {@link MessagePublisher}
     *
     * @param publisher - ID of {@link Microservice}
     * @return
     */
    public MessagePublisher getPublisher(String publisher) {
        return publishers.get(publisher);
    }

    /**
     * returns {@link MessageReceiver}
     *
     * @param receiver - ID of {@link Microservice}
     * @return
     */
    public MessageReceiver getReceiver(String receiver) {
        return receivers.get(receiver);
    }

    /**
     * returns next generated message id
     *
     * @return
     */
    public synchronized String getNextId() {
        return idGenerator.getNextId();
    }

    /**
     * returns routes
     *
     * @return
     */
    public synchronized Map<String, Route> getRoutes() {
        return microserviceManager.getRoutes();
    }
}
