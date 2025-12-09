package es.ulpgc.bigdata.ingestion.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DocumentDownloader {

    private static final Logger log = LoggerFactory.getLogger(DocumentDownloader.class);

    public String download(String documentId) {
        // TODO: implement real logic (HTTP download or local corpus).
        log.info("Mock downloading document {}", documentId);
        return "Content for document " + documentId + ". Replace with real source.";
    }
}
