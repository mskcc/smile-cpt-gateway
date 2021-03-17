package org.mskcc.cmo.metadb.cpt_gateway;

import java.util.concurrent.CountDownLatch;
import org.mskcc.cmo.messaging.Gateway;
import org.mskcc.cmo.metadb.cpt_gateway.service.MessageHandlingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"org.mskcc.cmo.messaging", "org.mskcc.cmo.metadb.cpt_gateway.*"})
public class CPTGatewayApp implements CommandLineRunner {

    @Autowired
    private Gateway messagingGateway;

    @Autowired
    private MessageHandlingService messageHandlingService;
    
    private Thread shutdownHook;
    final CountDownLatch cptGatewayClose = new CountDownLatch(1);

    @Override
    public void run(String... args) throws Exception {
        System.out.println("Starting up MetaDB CPT Gateway...");
        try {
            installShutdownHook();
            messagingGateway.connect();
            messageHandlingService.initialize(messagingGateway);
            System.out.println("Starting up MetaDB CPT Gateway complete...");
            cptGatewayClose.await();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        }
    }

    private void installShutdownHook() {
        shutdownHook =
            new Thread() {
                public void run() {
                    System.err.printf("\nCaught CTRL-C, shutting down gracefully...\n");
                    try {
                        messagingGateway.shutdown();
                        messageHandlingService.shutdown();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    cptGatewayClose.countDown();
                }
            };
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    public static void main(String[] args) {
        SpringApplication.run(CPTGatewayApp.class, args);
    }
}
