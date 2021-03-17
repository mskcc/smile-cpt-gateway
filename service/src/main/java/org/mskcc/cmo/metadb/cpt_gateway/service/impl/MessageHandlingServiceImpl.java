package org.mskcc.cmo.metadb.cpt_gateway.service.impl;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import org.mskcc.cmo.messaging.Gateway;
import org.mskcc.cmo.messaging.MessageConsumer;
import org.mskcc.cmo.metadb.cpt_gateway.service.CPTService;
import org.mskcc.cmo.metadb.cpt_gateway.service.MessageHandlingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class MessageHandlingServiceImpl implements MessageHandlingService {

    @Value("${cmo.new_request_topic}")
    private String CMO_NEW_REQUEST_TOPIC;

    @Value("${num.new_request_handler_threads}")
    private int NUM_NEW_REQUEST_HANDLERS;

    @Autowired
    private CPTService cptService;

    private static boolean initialized = false;
    private static volatile boolean shutdownInitiated;
    private static final ExecutorService exec = Executors.newCachedThreadPool();
    private static final BlockingQueue<String> newRequestQueue =
        new LinkedBlockingQueue<String>();
    private static CountDownLatch newRequestHandlerShutdownLatch;
    private static Gateway messagingGateway;

    private class NewCMORequestHandler implements Runnable {

        final Phaser phaser;
        boolean interrupted = false;

        NewCMORequestHandler(Phaser phaser) {
            this.phaser = phaser;
        }

        @Override
        public void run() {
            phaser.arrive();
            while (true) {
                try {
                    String request = newRequestQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (request != null) {
                        cptService.pushCMORequest(request);
                    }
                    if (interrupted && newRequestQueue.isEmpty()) {
                        break;
                    }
                } catch (InterruptedException e) {
                    interrupted = true;
                } catch (Exception e) {
                    System.err.printf("Error during request handling: %s\n", e.getMessage());
                    e.printStackTrace();
                }
            }
            newRequestHandlerShutdownLatch.countDown();
        }
    }

    @Override
    public void initialize(Gateway gateway) throws Exception {
        if (!initialized) {
            messagingGateway = gateway;
            setupCMONewRequestSubscriber(messagingGateway, this);
            initializeNewRequestHandlers();
            initialized = true;
        } else {
            System.err.printf("Messaging Handler Service has already been initialized, ignoring request.\n");
        }
    }

    @Override
    public void newRequestHandler(String request) throws Exception {
        if (!initialized) {
            throw new IllegalStateException("Message Handling Service has not been initialized");
        }
        if (!shutdownInitiated) {
            newRequestQueue.put(request);
        } else {
            System.err.printf("Shutdown initiated, not accepting request: %s\n", request);
            throw new IllegalStateException("Shutdown initiated, not handling any more requests");
        }
    }

    @Override
    public void shutdown() throws Exception {
        if (!initialized) {
            throw new IllegalStateException("Message Handling Service has not been initialized");
        }
        exec.shutdownNow();
        newRequestHandlerShutdownLatch.await();
        shutdownInitiated = true;
    }

    private void initializeNewRequestHandlers() throws Exception {
        newRequestHandlerShutdownLatch = new CountDownLatch(NUM_NEW_REQUEST_HANDLERS);
        final Phaser newRequestPhaser = new Phaser();
        newRequestPhaser.register();
        for (int lc = 0; lc < NUM_NEW_REQUEST_HANDLERS; lc++) {
            newRequestPhaser.register();
            exec.execute(new NewCMORequestHandler(newRequestPhaser));
        }
        newRequestPhaser.arriveAndAwaitAdvance();
    }

    private void setupCMONewRequestSubscriber(Gateway gateway, MessageHandlingService messageHandlingService)
        throws Exception {
        gateway.subscribe(CMO_NEW_REQUEST_TOPIC, Object.class, new MessageConsumer() {
            public void onMessage(Object message) {
                try {
                    messageHandlingService.newRequestHandler(message.toString());
                } catch (Exception e) {
                    System.err.printf("Cannot process CMO_NEW_REQUEST:\n%s\n", message);
                    System.err.printf("Exception during processing:\n%s\n", e.getMessage());
                    e.printStackTrace();
                }
            }
        });
    }
}
