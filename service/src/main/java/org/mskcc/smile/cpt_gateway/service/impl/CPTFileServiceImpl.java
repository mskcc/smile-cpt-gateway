package org.mskcc.smile.cpt_gateway.service.impl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import org.mskcc.smile.cpt_gateway.service.CPTFileService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class CPTFileServiceImpl implements CPTFileService {

    @Value("${cpt.cmo_project_request_post_failures_filepath}")
    private String filePath;

    @Override
    public void saveCMOProjectRequestPostFailure(String reason, String postContent) throws IOException {
        File logFile = new File(filePath);
        if (!logFile.exists()) {
            logFile.createNewFile();
        }
        BufferedWriter postFailureFile = new BufferedWriter(new FileWriter(logFile, true));
        postFailureFile.write(generatePostFailureRecord(reason, postContent));
        postFailureFile.close();
    }

    private String generatePostFailureRecord(String reason, String postContent) {
        String now = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE);
        StringBuilder builder = new StringBuilder();
        builder.append(now)
            .append("\t")
            .append(reason)
            .append("\t")
            .append(postContent)
            .append("\n");
        return builder.toString();
    }
}
