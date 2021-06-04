package org.mskcc.cmo.metadb.cpt_gateway.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.Message;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mskcc.cmo.messaging.Gateway;
import org.mskcc.cmo.messaging.MessageConsumer;
import org.mskcc.cmo.metadb.cpt_gateway.service.CPTService;
import org.mskcc.cmo.metadb.cpt_gateway.service.MessageHandlingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class MessageHandlingServiceImpl implements MessageHandlingService {
    private static final Log LOG = LogFactory.getLog(MessageHandlingServiceImpl.class);

    @Value("${cmo.new_request_topic}")
    private String CMO_NEW_REQUEST_TOPIC;

    @Value("${igo.request_status_topic}")
    private String IGO_REQUEST_STATUS_TOPIC;

    @Value("${num.new_request_handler_threads}")
    private int NUM_NEW_REQUEST_HANDLERS;

    @Value("${num.request_status_handler_threads}")
    private int NUM_REQUEST_STATUS_HANDLERS;

    @Value("${cpt.create_record_url}")
    private String CPT_CREATE_RECORD_URL;

    @Value("${cpt.sample_status_record_url}")
    private String CPT_SAMPLE_STATUS_RECORD_URL;

    @Autowired
    private CPTService cptService;

    private static boolean initialized = false;
    private static volatile boolean shutdownInitiated;
    private static final ExecutorService exec = Executors.newCachedThreadPool();
    private static final BlockingQueue<String> newRequestQueue =
        new LinkedBlockingQueue<String>();
    private static CountDownLatch newRequestHandlerShutdownLatch;
    private static final BlockingQueue<String> requestStatusQueue =
        new LinkedBlockingQueue<String>();
    private static CountDownLatch requestStatusHandlerShutdownLatch;
    private static Gateway messagingGateway;
    private final ObjectMapper mapper = new ObjectMapper();

    private class CPTHandler implements Runnable {

        final Phaser phaser;
        boolean interrupted = false;
        final String cptDestination;
        final BlockingQueue<String> requestQueue;
        final CountDownLatch shutdownLatch;

        CPTHandler(Phaser phaser, String cptDestination,
                   BlockingQueue<String> requestQueue, CountDownLatch shutdownLatch) {
            this.phaser = phaser;
            this.cptDestination = cptDestination;
            this.requestQueue = requestQueue;
            this.shutdownLatch = shutdownLatch;
        }

        @Override
        public void run() {
            phaser.arrive();
            while (true) {
                try {
                    String request = requestQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (request != null) {
                        cptService.pushRecord(request, cptDestination);
                    }
                    if (interrupted && requestQueue.isEmpty()) {
                        break;
                    }
                } catch (InterruptedException e) {
                    interrupted = true;
                } catch (Exception e) {
                    LOG.error("Error during request handling: ", e);
                    e.printStackTrace();
                }
            }
            shutdownLatch.countDown();
        }
    }

    @Override
    public void initialize(Gateway gateway) throws Exception {
        if (!initialized) {
            messagingGateway = gateway;
            setupCMONewRequestSubscriber(messagingGateway, this);
            setupIGORequestStatusSubscriber(messagingGateway, this);
            initializeNewRequestHandlers();
            initializeRequestStatusHandlers();
            initialized = true;
        } else {
            LOG.error("Messaging Handler Service has already been initialized, ignoring request");
        }
    }

    @Override
    public void newRequestHandler(String newRequest) throws Exception {
        if (!initialized) {
            throw new IllegalStateException("Message Handling Service has not been initialized");
        }
        if (!shutdownInitiated) {
            newRequestQueue.put(newRequest);
        } else {
            LOG.error("Shutdown initiated, not accepting request: \n" + newRequest);
            throw new IllegalStateException("Shutdown initiated, not handling any more requests");
        }
    }

    @Override
    public void requestStatusHandler(String requestStatus) throws Exception {
        if (!initialized) {
            throw new IllegalStateException("Message Handling Service has not been initialized");
        }
        if (!shutdownInitiated) {
            requestStatusQueue.put(requestStatus);
        } else {
            LOG.error("Shutdown initiated, not accepting request");
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
        requestStatusHandlerShutdownLatch.await();
        shutdownInitiated = true;
    }

    private void initializeNewRequestHandlers() throws Exception {
        newRequestHandlerShutdownLatch = new CountDownLatch(NUM_NEW_REQUEST_HANDLERS);
        final Phaser newRequestPhaser = new Phaser();
        newRequestPhaser.register();
        for (int lc = 0; lc < NUM_NEW_REQUEST_HANDLERS; lc++) {
            newRequestPhaser.register();
            exec.execute(new CPTHandler(newRequestPhaser, CPT_CREATE_RECORD_URL,
                                        newRequestQueue, newRequestHandlerShutdownLatch));
        }
        newRequestPhaser.arriveAndAwaitAdvance();
    }

    private void initializeRequestStatusHandlers() throws Exception {
        requestStatusHandlerShutdownLatch = new CountDownLatch(NUM_REQUEST_STATUS_HANDLERS);
        final Phaser requestStatusPhaser = new Phaser();
        requestStatusPhaser.register();
        for (int lc = 0; lc < NUM_REQUEST_STATUS_HANDLERS; lc++) {
            requestStatusPhaser.register();
            exec.execute(new CPTHandler(requestStatusPhaser, CPT_SAMPLE_STATUS_RECORD_URL,
                                        requestStatusQueue, requestStatusHandlerShutdownLatch));
        }
        requestStatusPhaser.arriveAndAwaitAdvance();
    }

    private void setupCMONewRequestSubscriber(Gateway gateway, MessageHandlingService messageHandlingService)
        throws Exception {
        gateway.subscribe(CMO_NEW_REQUEST_TOPIC, Object.class, new MessageConsumer() {
            public void onMessage(Message msg, Object message) {
                try {
                    messageHandlingService.newRequestHandler(
                            mapper.readValue(
                            new String(msg.getData(), StandardCharsets.UTF_8),
                            String.class)
                    );
                } catch (Exception e) {
                    LOG.error("Cannot process CMO_NEW_REQUEST: " + message.toString(), e);
                }
            }
        });
    }
    private void setupIGORequestStatusSubscriber(Gateway gateway,
                                                 MessageHandlingService messageHandlingService)
        throws Exception {
        gateway.subscribe(IGO_REQUEST_STATUS_TOPIC, Object.class, new MessageConsumer() {
            public void onMessage(Message msg, Object message) {
                try {
                    messageHandlingService.requestStatusHandler(
                            mapper.readValue(
                            new String(msg.getData(), StandardCharsets.UTF_8),
                            String.class)
                    );
                } catch (Exception e) {
                    LOG.error("Cannot process IGO_REQUEST_STATUS_TOPIC: " + message.toString(), e);
                }
            }
        });
    }

}
