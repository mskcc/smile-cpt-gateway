package org.mskcc.smile.cpt_gateway.service;

import java.io.IOException;

public interface CPTFileService {
    
    void saveCPTPostFailure(String reason, String postContent) throws IOException;
}
