package org.mskcc.smile.cpt_gateway.service;

public interface CPTService {

    void pushRecord(String request, String cptDestination) throws Exception;
}
