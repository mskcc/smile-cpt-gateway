package org.mskcc.cmo.metadb.cpt_gateway.service;

public interface CPTService {

    void pushRecord(String request, String cptDestination) throws Exception;
}
