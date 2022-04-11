package org.mskcc.smile.cpt_gateway.service;

import org.mskcc.cmo.messaging.Gateway;

public interface MessageHandlingService {

    void initialize(Gateway gateway) throws Exception;

    void promotedRequestHandler(String promotedRequest) throws Exception;

    void newRequestHandler(String newRequest) throws Exception;
    
    void updateRequestHandler(String updateRequest) throws Exception;

    void updateSampleHandler(String updateSample) throws Exception;

    void requestStatusHandler(String requestStatus) throws Exception;

    void shutdown() throws Exception;
}
