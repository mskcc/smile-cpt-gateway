package org.mskcc.smile.cpt_gateway.service.impl;

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
import org.mskcc.smile.cpt_gateway.service.CPTService;
import org.mskcc.smile.cpt_gateway.service.MessageHandlingService;
import org.mskcc.smile.cpt_gateway.service.impl.CPTServiceImpl.CPTRecordDest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class MessageHandlingServiceImpl implements MessageHandlingService {
    private static final Log LOG = LogFactory.getLog(MessageHandlingServiceImpl.class);

    @Value("${cmo.promoted_request_topic}")
    private String CMO_PROMOTED_REQUEST_TOPIC;

    @Value("${cmo.new_request_topic}")
    private String CMO_NEW_REQUEST_TOPIC;

    @Value("${cmo.update_request_topic}")
    private String CMO_UPDATE_REQUEST_TOPIC;

    @Value("${cmo.update_sample_topic}")
    private String CMO_UPDATE_SAMPLE_TOPIC;

    @Value("${igo.request_status_topic}")
    private String IGO_REQUEST_STATUS_TOPIC;
    
    @Value("${num.promoted_request_handler_threads}")
    private int NUM_PROMOTED_REQUEST_HANDLERS;

    @Value("${num.new_request_handler_threads}")
    private int NUM_NEW_REQUEST_HANDLERS;

    @Value("${num.update_request_handler_threads}")
    private int NUM_UPDATE_REQUEST_HANDLERS;

    @Value("${num.update_sample_handler_threads}")
    private int NUM_UPDATE_SAMPLE_HANDLERS;

    @Value("${num.request_status_handler_threads}")
    private int NUM_REQUEST_STATUS_HANDLERS;

    @Autowired
    private CPTService cptService;

    private static boolean initialized = false;
    private static volatile boolean shutdownInitiated;
    private static final ExecutorService exec = Executors.newCachedThreadPool();
    private static final BlockingQueue<String> promotedRequestQueue =
        new LinkedBlockingQueue<String>();
    private static CountDownLatch promotedRequestHandlerShutdownLatch;
    private static final BlockingQueue<String> newRequestQueue =
        new LinkedBlockingQueue<String>();
    private static CountDownLatch newRequestHandlerShutdownLatch;
    private static final BlockingQueue<String> updateRequestQueue =
        new LinkedBlockingQueue<String>();
    private static CountDownLatch updateRequestHandlerShutdownLatch;
    private static final BlockingQueue<String> updateSampleQueue =
        new LinkedBlockingQueue<String>();
    private static CountDownLatch updateSampleHandlerShutdownLatch;
    private static final BlockingQueue<String> requestStatusQueue =
        new LinkedBlockingQueue<String>();
    private static CountDownLatch requestStatusHandlerShutdownLatch;
    private static Gateway messagingGateway;
    private final ObjectMapper mapper = new ObjectMapper();

    private class CPTHandler implements Runnable {

        final Phaser phaser;
        boolean interrupted = false;
        final CPTRecordDest cptRecordDest;
        final BlockingQueue<String> requestQueue;
        final CountDownLatch shutdownLatch;

        CPTHandler(Phaser phaser, CPTRecordDest cptRecordDest,
                   BlockingQueue<String> requestQueue, CountDownLatch shutdownLatch) {
            this.phaser = phaser;
            this.cptRecordDest = cptRecordDest;
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
                        cptService.pushRecord(request, cptRecordDest);
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
            setupCMOPromotedRequestSubscriber(messagingGateway, this);
            initializePromotedRequestHandlers();
            setupCMONewRequestSubscriber(messagingGateway, this);
            initializeNewRequestHandlers();
            setupCMOUpdateRequestSubscriber(messagingGateway, this);
            initializeUpdateRequestHandlers();
            setupCMOUpdateSampleSubscriber(messagingGateway, this);
            initializeUpdateSampleHandlers();
            setupIGORequestStatusSubscriber(messagingGateway, this);
            initializeRequestStatusHandlers();
            initialized = true;
        } else {
            LOG.error("Messaging Handler Service has already been initialized, ignoring request");
        }
    }

    @Override
    public void promotedRequestHandler(String promotedRequest) throws Exception {
        if (!initialized) {
            throw new IllegalStateException("Message Handling Service has not been initialized");
        }
        if (!shutdownInitiated) {
            promotedRequestQueue.put(promotedRequest);
        } else {
            LOG.error("Shutdown initiated, not accepting request: \n" + promotedRequest);
            throw new IllegalStateException("Shutdown initiated, not handling any more requests");
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
    public void updateRequestHandler(String updateRequest) throws Exception {
        if (!initialized) {
            throw new IllegalStateException("Message Handling Service has not been initialized");
        }
        if (!shutdownInitiated) {
            updateRequestQueue.put(updateRequest);
        } else {
            LOG.error("Shutdown initiated, not accepting request: \n" + updateRequest);
            throw new IllegalStateException("Shutdown initiated, not handling any more requests");
        }
    }

    @Override
    public void updateSampleHandler(String updateSample) throws Exception {
        if (!initialized) {
            throw new IllegalStateException("Message Handling Service has not been initialized");
        }
        if (!shutdownInitiated) {
            updateSampleQueue.put(updateSample);
        } else {
            LOG.error("Shutdown initiated, not accepting request: \n" + updateSample);
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
        promotedRequestHandlerShutdownLatch.await();
        newRequestHandlerShutdownLatch.await();
        updateRequestHandlerShutdownLatch.await();
        updateSampleHandlerShutdownLatch.await();
        requestStatusHandlerShutdownLatch.await();
        shutdownInitiated = true;
    }

    private void initializePromotedRequestHandlers() throws Exception {
        promotedRequestHandlerShutdownLatch = new CountDownLatch(NUM_PROMOTED_REQUEST_HANDLERS);
        final Phaser promotedRequestPhaser = new Phaser();
        promotedRequestPhaser.register();
        for (int lc = 0; lc < NUM_PROMOTED_REQUEST_HANDLERS; lc++) {
            promotedRequestPhaser.register();
            exec.execute(new CPTHandler(promotedRequestPhaser, CPTRecordDest.PROMOTED_REQUEST_RECORD_DEST,
                                        promotedRequestQueue, promotedRequestHandlerShutdownLatch));
        }
        promotedRequestPhaser.arriveAndAwaitAdvance();
    }

    private void initializeNewRequestHandlers() throws Exception {
        newRequestHandlerShutdownLatch = new CountDownLatch(NUM_NEW_REQUEST_HANDLERS);
        final Phaser newRequestPhaser = new Phaser();
        newRequestPhaser.register();
        for (int lc = 0; lc < NUM_NEW_REQUEST_HANDLERS; lc++) {
            newRequestPhaser.register();
            exec.execute(new CPTHandler(newRequestPhaser, CPTRecordDest.NEW_REQUEST_RECORD_DEST,
                                        newRequestQueue, newRequestHandlerShutdownLatch));
        }
        newRequestPhaser.arriveAndAwaitAdvance();
    }

    private void initializeUpdateRequestHandlers() throws Exception {
        updateRequestHandlerShutdownLatch = new CountDownLatch(NUM_UPDATE_REQUEST_HANDLERS);
        final Phaser updateRequestPhaser = new Phaser();
        updateRequestPhaser.register();
        for (int lc = 0; lc < NUM_UPDATE_REQUEST_HANDLERS; lc++) {
            updateRequestPhaser.register();
            exec.execute(new CPTHandler(updateRequestPhaser, CPTRecordDest.UPDATE_REQUEST_RECORD_DEST,
                                        updateRequestQueue, updateRequestHandlerShutdownLatch));
        }
        updateRequestPhaser.arriveAndAwaitAdvance();
    }

    private void initializeUpdateSampleHandlers() throws Exception {
        updateSampleHandlerShutdownLatch = new CountDownLatch(NUM_UPDATE_SAMPLE_HANDLERS);
        final Phaser updateSamplePhaser = new Phaser();
        updateSamplePhaser.register();
        for (int lc = 0; lc < NUM_UPDATE_SAMPLE_HANDLERS; lc++) {
            updateSamplePhaser.register();
            exec.execute(new CPTHandler(updateSamplePhaser, CPTRecordDest.UPDATE_SAMPLE_RECORD_DEST,
                                        updateSampleQueue, updateSampleHandlerShutdownLatch));
        }
        updateSamplePhaser.arriveAndAwaitAdvance();
    }

    private void initializeRequestStatusHandlers() throws Exception {
        requestStatusHandlerShutdownLatch = new CountDownLatch(NUM_REQUEST_STATUS_HANDLERS);
        final Phaser requestStatusPhaser = new Phaser();
        requestStatusPhaser.register();
        for (int lc = 0; lc < NUM_REQUEST_STATUS_HANDLERS; lc++) {
            requestStatusPhaser.register();
            exec.execute(new CPTHandler(requestStatusPhaser, CPTRecordDest.SAMPLE_STATUS_RECORD_DEST,
                                        requestStatusQueue, requestStatusHandlerShutdownLatch));
        }
        requestStatusPhaser.arriveAndAwaitAdvance();
    }

    private void setupCMOPromotedRequestSubscriber(Gateway gateway,
                                                   MessageHandlingService messageHandlingService)
        throws Exception {
        gateway.subscribe(CMO_PROMOTED_REQUEST_TOPIC, Object.class, new MessageConsumer() {
            public void onMessage(Message msg, Object message) {
                try {
                    messageHandlingService.promotedRequestHandler(
                            mapper.readValue(
                            new String(msg.getData(), StandardCharsets.UTF_8),
                            String.class)
                    );
                } catch (Exception e) {
                    LOG.error("Cannot process CMO_PROMOTED_REQUEST: " + message.toString(), e);
                }
            }
        });
    }

    private void setupCMONewRequestSubscriber(Gateway gateway,
                                              MessageHandlingService messageHandlingService)
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

    private void setupCMOUpdateRequestSubscriber(Gateway gateway,
                                                 MessageHandlingService messageHandlingService)
        throws Exception {
        gateway.subscribe(CMO_UPDATE_REQUEST_TOPIC, Object.class, new MessageConsumer() {
            public void onMessage(Message msg, Object message) {
                try {
                    messageHandlingService.updateRequestHandler(
                            mapper.readValue(
                            new String(msg.getData(), StandardCharsets.UTF_8),
                            String.class)
                    );
                } catch (Exception e) {
                    LOG.error("Cannot process CMO_UPDATE_REQUEST: " + message.toString(), e);
                }
            }
        });
    }

    private void setupCMOUpdateSampleSubscriber(Gateway gateway,
                                                MessageHandlingService messageHandlingService)
        throws Exception {
        gateway.subscribe(CMO_UPDATE_SAMPLE_TOPIC, Object.class, new MessageConsumer() {
            public void onMessage(Message msg, Object message) {
                try {
                    messageHandlingService.updateSampleHandler(
                            mapper.readValue(
                            new String(msg.getData(), StandardCharsets.UTF_8),
                            String.class)
                    );
                } catch (Exception e) {
                    LOG.error("Cannot process CMO_UPDATE_SAMPLE: " + message.toString(), e);
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
