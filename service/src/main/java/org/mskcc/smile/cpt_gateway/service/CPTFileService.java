package org.mskcc.smile.cpt_gateway.service;

import java.io.IOException;

public interface CPTFileService {
    
    void saveCMOProjectRequestPostFailure(String reason, String postContent) throws IOException;
}
