package es.ulpgc.bigdata.ingestion.api.dto;

/**
 * Represents a document stored in the local datalake partition.
 */
public class DocumentInfoResponse {

    public String documentId;
    public String path;

    public DocumentInfoResponse(String documentId, String path) {
        this.documentId = documentId;
        this.path = path;
    }
}
