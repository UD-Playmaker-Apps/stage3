package es.ulpgc.bigdata.ingestion.api.dto;

public class IngestionStatusResponse {
    public String documentId;
    public String status;

    public IngestionStatusResponse(String documentId, String status) {
        this.documentId = documentId;
        this.status = status;
    }
}
