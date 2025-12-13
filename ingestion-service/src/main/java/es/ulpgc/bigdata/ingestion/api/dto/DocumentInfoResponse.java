package es.ulpgc.bigdata.ingestion.api.dto;

public class DocumentInfoResponse {
    public String documentId;
    public String path;

    public DocumentInfoResponse(String documentId, String path) {
        this.documentId = documentId;
        this.path = path;
    }
}
