package es.ulpgc.bigdata.ingestion.core;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Core service that performs the ingestion pipeline:
 *
 * 1) Download the document 2) Store it in the local datalake partition 3)
 * Replicate the document to R-1 other ingestion nodes 4) Publish an event to
 * the Message Broker so indexers can process it
 *
 * This service corresponds to the "Ingestion Service" in the Stage-3
 * architecture.
 */
public class IngestionService {

    private static final Logger log = LoggerFactory.getLogger(IngestionService.class);

    private final DatalakePartition datalake;
    private final DocumentDownloader downloader;
    private final ReplicationManager replicationManager;
    private final BrokerPublisher brokerPublisher;

    // Tracks ingestion progress per document ID.
    // This avoids duplicate work and helps debugging.
    private final Map<String, IngestionStatus> statusMap = new ConcurrentHashMap<>();

    public IngestionService(DatalakePartition datalake,
            DocumentDownloader downloader,
            ReplicationManager replicationManager,
            BrokerPublisher brokerPublisher) {
        this.datalake = datalake;
        this.downloader = downloader;
        this.replicationManager = replicationManager;
        this.brokerPublisher = brokerPublisher;
    }

    /**
     * Executes the full ingestion lifecycle for a given document. This method
     * is idempotent: if a document has already been processed, the service will
     * skip redundant work.
     */
    public void ingest(String documentId) {

        // If already completed, do nothing (idempotent ingestion)
        IngestionStatus current = statusMap.get(documentId);
        if (current == IngestionStatus.COMPLETED) {
            log.info("Document {} already ingested. Skipping.", documentId);
            return;
        }

        statusMap.put(documentId, IngestionStatus.DOWNLOADING);

        try {
            // 1) Download document content from external source
            String content = downloader.download(documentId);

            // 2) Store locally in this node's datalake partition
            statusMap.put(documentId, IngestionStatus.STORING);
            Path localPath = datalake.storeDocument(documentId, content);

            // 3) Send the document to R-1 peers so distributed datalake is maintained
            statusMap.put(documentId, IngestionStatus.REPLICATING);
            replicationManager.replicate(documentId, content);

            // 4) Publish an event indicating that ingestion is complete
            //    Indexing Services will consume this event
            statusMap.put(documentId, IngestionStatus.PUBLISHING_EVENT);
            brokerPublisher.publishDocumentIngested(documentId, localPath.toString());

            // Ingestion completed successfully
            statusMap.put(documentId, IngestionStatus.COMPLETED);

        } catch (IOException e) {
            log.error("I/O error ingesting {}: {}", documentId, e.getMessage(), e);
            statusMap.put(documentId, IngestionStatus.FAILED);
        } catch (RuntimeException e) {
            log.error("Unexpected error ingesting {}: {}", documentId, e.getMessage(), e);
            statusMap.put(documentId, IngestionStatus.FAILED);
        }
    }

    /**
     * Called by peer ingestion nodes to store a replica. Note: replica storage
     * must NOT publish another event to the broker, otherwise infinite loops
     * would occur.
     */
    public void storeReplica(String documentId, String content) {
        try {
            datalake.storeDocument(documentId, content);
        } catch (IOException e) {
            log.error("I/O error storing replica for {}: {}", documentId, e.getMessage(), e);
            throw new RuntimeException(e);
        } catch (RuntimeException e) {
            log.error("Unexpected error storing replica for {}: {}", documentId, e.getMessage(), e);
            throw e;
        }
    }

    public IngestionStatus getStatus(String documentId) {
        return statusMap.getOrDefault(documentId, IngestionStatus.UNKNOWN);
    }

    public Map<String, Path> listDocuments() {
        return datalake.listDocuments();
    }
}
