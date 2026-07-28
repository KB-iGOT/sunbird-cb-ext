package org.sunbird.workallocation.service;

import org.sunbird.workallocation.model.PdfGeneratorRequest;

import java.io.IOException;
import java.util.Map;

public interface PdfGeneratorService {
    public byte[] generatePdf(PdfGeneratorRequest request) throws Exception;
    public byte[] generatePdf(String woId) throws Exception;
    public String getPublishedPdfLink(String woId);
    public String generatePdfAndGetFilePath(String woId);
    public  byte[] getBatchSessionQRPdf(String authUserToken,String courseId,String batchId) throws IOException;
    public Map<String, Object> getQRStatus(String courseId, String batchId);
    public byte[] getBatchEnrollmentQRPdf(String authUserToken, String courseId, String batchId) throws IOException;
}
