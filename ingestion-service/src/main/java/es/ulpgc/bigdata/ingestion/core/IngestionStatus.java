package es.ulpgc.bigdata.ingestion.core;

public enum IngestionStatus {
    UNKNOWN, DOWNLOADING, STORING, REPLICATING, PUBLISHING_EVENT, COMPLETED, FAILED
}
