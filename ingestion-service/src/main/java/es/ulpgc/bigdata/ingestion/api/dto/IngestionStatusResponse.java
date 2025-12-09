package es.ulpgc.bigdata.ingestion.api.dto;

/**
 * Response model used to return ingestion status to clients.
 */
public class IngestionStatusResponse {

    public String documentId;
    public String status;

    public IngestionStatusResponse(String documentId, String status) {
        this.documentId = documentId;
        this.status = status;
    }
}
