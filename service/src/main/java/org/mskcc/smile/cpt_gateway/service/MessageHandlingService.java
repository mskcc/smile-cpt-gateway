package org.mskcc.smile.cpt_gateway.service;

import org.mskcc.cmo.messaging.Gateway;

public interface MessageHandlingService {

    void initialize(Gateway gateway) throws Exception;

    void newRequestHandler(String request) throws Exception;

    void requestStatusHandler(String requestStatus) throws Exception;

    void shutdown() throws Exception;
}
