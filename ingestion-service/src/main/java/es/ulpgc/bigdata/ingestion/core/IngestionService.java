package es.ulpgc.bigdata.ingestion.core;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Core service that performs the ingestion pipeline:
 * 1) Download document via GutenbergDownloader
 * 2) Store locally (header/body)
 * 3) Replicate to R-1 peers (POST JSON)
 * 4) Publish an event to the Message Broker
 */
public class IngestionService {

    private static final Logger log = LoggerFactory.getLogger(IngestionService.class);

    private final DatalakePartition datalake;
    private final DocumentDownloader downloader;
    private final ReplicationManager replicationManager;
    private final BrokerPublisher brokerPublisher;

    private final Map<String, IngestionStatus> statusMap = new ConcurrentHashMap<>();
    private final Gson gson = new Gson();

    public IngestionService(DatalakePartition datalake,
                            DocumentDownloader downloader,
                            ReplicationManager replicationManager,
                            BrokerPublisher brokerPublisher) {
        this.datalake = datalake;
        this.downloader = downloader;
        this.replicationManager = replicationManager;
        this.brokerPublisher = brokerPublisher;
    }

    public void ingest(String documentId) {
        IngestionStatus current = statusMap.get(documentId);
        if (current == IngestionStatus.COMPLETED) {
            log.info("Document {} already ingested. Skipping.", documentId);
            return;
        }

        statusMap.put(documentId, IngestionStatus.DOWNLOADING);

        try {
            DocumentDownloader.DownloadResult dl = downloader.download(documentId);

            statusMap.put(documentId, IngestionStatus.STORING);
            Path localPath = datalake.storeDocument(documentId, dl.header, dl.body, dl.sourceUrl);

            statusMap.put(documentId, IngestionStatus.REPLICATING);
            replicationManager.replicate(documentId, dl.header, dl.body, dl.sourceUrl);

            statusMap.put(documentId, IngestionStatus.PUBLISHING_EVENT);
            brokerPublisher.publishDocumentIngested(documentId, localPath.toString(), dl.sourceUrl);

            statusMap.put(documentId, IngestionStatus.COMPLETED);

        } catch (IOException e) {
            log.error("I/O error ingesting {}: {}", documentId, e.getMessage(), e);
            statusMap.put(documentId, IngestionStatus.FAILED);
            throw new RuntimeException(e);
        } catch (RuntimeException e) {
            log.error("Unexpected error ingesting {}: {}", documentId, e.getMessage(), e);
            statusMap.put(documentId, IngestionStatus.FAILED);
            throw e;
        }
    }

    /**
     * Receives replica JSON posted by a peer.
     * JSON expected: { "header": "...", "body": "...", "sourceUrl": "..." }
     */
    public void storeReplicaFromJson(String documentId, String jsonPayload) {
        try {
            JsonObject obj = JsonParser.parseString(jsonPayload).getAsJsonObject();
            String header = obj.has("header") ? obj.get("header").getAsString() : "";
            String body = obj.has("body") ? obj.get("body").getAsString() : "";
            String sourceUrl = obj.has("sourceUrl") ? obj.get("sourceUrl").getAsString() : "";
            datalake.storeReplica(documentId, header, body, sourceUrl);
        } catch (Exception e) {
            throw new RuntimeException("Failed to store replica: " + e.getMessage(), e);
        }
    }

    public IngestionStatus getStatus(String documentId) {
        return statusMap.getOrDefault(documentId, IngestionStatus.UNKNOWN);
    }

    public Map<String, Path> listDocuments() {
        return datalake.listDocuments();
    }
}
