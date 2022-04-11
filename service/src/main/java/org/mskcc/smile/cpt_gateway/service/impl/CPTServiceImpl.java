package org.mskcc.smile.cpt_gateway.service.impl;

import java.security.cert.X509Certificate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.mskcc.smile.cpt_gateway.service.CPTFileService;
import org.mskcc.smile.cpt_gateway.service.CPTService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class CPTServiceImpl implements CPTService {

    private static final Pattern CPT_TOKEN_REGEX = Pattern.compile(".*token=(\\w+).*");
    private static final Pattern IGO_REQUEST_TRACKER_ID_REGEX = Pattern.compile(".*requestId\":\"(\\w+).*");
    private static final Pattern IGO_REQUEST_ID_REGEX = Pattern.compile(".*igoRequestId\":\"(\\w+).*");
    private static final Pattern IGO_SAMPLE_ID_REGEX = Pattern.compile(".*sampleName\":\"(\\w+).*");

    @Value("${cpt.promoted_request_record_url}")
    private String CPT_PROMOTED_REQUEST_RECORD_URL;

    @Value("${cpt.new_request_record_url}")
    private String CPT_NEW_REQUEST_RECORD_URL;

    @Value("${cpt.update_request_record_url:}")
    private String CPT_UPDATE_REQUEST_RECORD_URL;

    @Value("${cpt.update_sample_record_url:}")
    private String CPT_UPDATE_SAMPLE_RECORD_URL;

    @Value("${cpt.sample_status_record_url}")
    private String CPT_SAMPLE_STATUS_RECORD_URL;

    @Value("${cpt.post_timeouts}")
    private int CPT_POST_TIMEOUTS;

    @Value("${cpt.session_token_url}")
    private String CPT_SESSION_TOKEN_URL;

    @Value("${cpt.authorization_token}")
    private String CPT_AUTHORIZATION_TOKEN;

    @Autowired
    CPTFileService cptFileService;

    private static final Log LOG = LogFactory.getLog(CPTServiceImpl.class);

    public static enum CPTRecordDest {
        PROMOTED_REQUEST_RECORD_DEST,
        NEW_REQUEST_RECORD_DEST,
        UPDATE_REQUEST_RECORD_DEST,
        UPDATE_SAMPLE_RECORD_DEST,
        SAMPLE_STATUS_RECORD_DEST;
    }

    @Override
    public void pushRecord(String record, CPTRecordDest recordDest) throws Exception {
        switch (recordDest) {
            case PROMOTED_REQUEST_RECORD_DEST:
                postToCPT(getEntityId(record, IGO_REQUEST_ID_REGEX),
                          getRequestPostBody(record), CPT_PROMOTED_REQUEST_RECORD_URL);
                break;
            case NEW_REQUEST_RECORD_DEST:
                postToCPT(getEntityId(record, IGO_REQUEST_ID_REGEX),
                          getRequestPostBody(record), CPT_NEW_REQUEST_RECORD_URL);
                break;
            case UPDATE_REQUEST_RECORD_DEST:
                postToCPT(getEntityId(record, IGO_REQUEST_ID_REGEX),
                          getRequestPostBody(record), CPT_UPDATE_REQUEST_RECORD_URL);
                break;
            case UPDATE_SAMPLE_RECORD_DEST:
                postToCPT(getEntityId(record, IGO_SAMPLE_ID_REGEX),
                          getSamplePostBody(record), CPT_UPDATE_SAMPLE_RECORD_URL);
                break;
            case SAMPLE_STATUS_RECORD_DEST:
                postToCPT(getEntityId(record, IGO_REQUEST_TRACKER_ID_REGEX),
                          getRequestPostBody(record), CPT_SAMPLE_STATUS_RECORD_URL);
                break;
            default:
                break;
        }
    }

    private String getSamplePostBody(String sampleRecord) {
        String igoSampleID = getEntityId(sampleRecord, IGO_SAMPLE_ID_REGEX);
        if (igoSampleID.length() > 0) {
            // this is to avoid filemaker data api error 1708
            String escapedSample = sampleRecord.replace("\"", "\\\"");
            return "{\"fieldData\":{\"igoSampleId\": \""
                + igoSampleID + "\",\"sampleJSON\": " + escapedSample + "}}";
        }
        throw new RuntimeException("Error parsing request, cannot find requestId.",
                                   new Throwable(sampleRecord));
    }

    private String getRequestPostBody(String requestRecord) {
        String igoRequestID = getEntityId(requestRecord, IGO_REQUEST_ID_REGEX);
        if (igoRequestID.length() > 0) {
            // this is to avoid filemaker data api error 1708
            String escapedRequest = requestRecord.replace("\"", "\\\"");
            return "{\"fieldData\":{\"projectBatchNumber\": \""
                + igoRequestID + "\",\"requestJSON\": " + escapedRequest + "}}";
        }
        throw new RuntimeException("Error parsing request, cannot find requestId.",
                                   new Throwable(requestRecord));
    }

    private String getEntityId(String entityRecord, Pattern regex) {
        Matcher matcher = regex.matcher(entityRecord);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "";
    }

    private void postToCPT(String entityId, String postBody, String recordDest) throws Exception {
        if (recordDest.isEmpty()) {
            return;
        }
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth(getSessionToken());
        try {
            HttpEntity requestEntity = new HttpEntity<Object>(postBody, headers);
            RestTemplate restTemplate = getRestTemplate();
            ResponseEntity responseEntity =
                restTemplate.exchange(recordDest,
                                      HttpMethod.POST, requestEntity, Object.class);
            if (!responseEntity.getStatusCode().is2xxSuccessful()) {
                cptFileService.saveCPTPostFailure(responseEntity.getStatusCode().toString(),
                                                  postBody);
                if (LOG.isInfoEnabled()) {
                    LOG.info("Unsuccessful postToCPT (Entity ID, CPT URL): "
                             + entityId + ", " + recordDest);
                }
            } else if (LOG.isInfoEnabled()) {
                LOG.info("Successful postToCPT (Entity ID, CPT URL): "
                         + entityId + ", " + recordDest);
            }
        } catch (Exception e) {
            if (postBody.equals("")) {
                cptFileService.saveCPTPostFailure(e.getMessage(), e.getCause().getMessage());
            } else {
                cptFileService.saveCPTPostFailure(e.getMessage(), postBody);
            }
        }
    }

    private String getSessionToken() throws Exception {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBasicAuth(CPT_AUTHORIZATION_TOKEN);
        HttpEntity requestEntity = new HttpEntity<Object>(headers);
        RestTemplate restTemplate = getRestTemplate();
        ResponseEntity responseEntity = restTemplate.exchange(CPT_SESSION_TOKEN_URL,
                                                              HttpMethod.POST, requestEntity, Object.class);
        if (responseEntity.getStatusCode().is2xxSuccessful()) {
            Matcher matcher = CPT_TOKEN_REGEX.matcher(responseEntity.getBody().toString());
            if (matcher.find()) {
                return matcher.group(1);
            }
        }
        throw new RuntimeException("CPT session token call failed, http status code: "
                                   + responseEntity.getStatusCode()
                                   + ", content: " + responseEntity.getBody());
    }

    private RestTemplate getRestTemplate() throws Exception {
        TrustStrategy acceptingTrustStrategy = (X509Certificate[] chain, String authType) -> true;
        HostnameVerifier hostnameVerifier = (s, sslSession) -> true;
        SSLContext sslContext = SSLContexts.custom().loadTrustMaterial(null, acceptingTrustStrategy).build();
        SSLConnectionSocketFactory csf = new SSLConnectionSocketFactory(sslContext, hostnameVerifier);
        CloseableHttpClient httpClient = HttpClients.custom().setSSLSocketFactory(csf).build();
        RequestConfig requestConfig = RequestConfig.custom()
            // time to wait for a connection from pool
            .setConnectionRequestTimeout(CPT_POST_TIMEOUTS)
            // time to establish connection with remote
            .setConnectTimeout(CPT_POST_TIMEOUTS)
            // time waiting for data
            .setSocketTimeout(CPT_POST_TIMEOUTS).build();
        HttpClientBuilder builder = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig);
        HttpComponentsClientHttpRequestFactory requestFactory =
            new HttpComponentsClientHttpRequestFactory(builder.build());
        requestFactory.setHttpClient(httpClient);
        return new RestTemplate(requestFactory);
    }
}
