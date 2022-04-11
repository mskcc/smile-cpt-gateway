package org.mskcc.smile.cpt_gateway.service;

import org.mskcc.smile.cpt_gateway.service.impl.CPTServiceImpl.CPTRecordDest;

public interface CPTService {

    void pushRecord(String record, CPTRecordDest recordDest) throws Exception;
}
