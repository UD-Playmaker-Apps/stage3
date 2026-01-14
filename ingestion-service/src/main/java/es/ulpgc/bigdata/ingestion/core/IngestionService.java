package es.ulpgc.bigdata.ingestion.core;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestionService {

    private static final Logger log = LoggerFactory.getLogger(IngestionService.class);

    private final DatalakePartition datalake;
    private final DocumentDownloader downloader;
    private final MetadataFetcher metadataFetcher;
    private final ReplicationManager replicationManager;
    private final BrokerPublisher brokerPublisher;
    private final Map<String, IngestionStatus> statusMap = new ConcurrentHashMap<>();
    private final Gson gson = new Gson();

    public IngestionService(DatalakePartition datalake,
                            DocumentDownloader downloader,
                            MetadataFetcher metadataFetcher,
                            ReplicationManager replicationManager,
                            BrokerPublisher brokerPublisher) {
        this.datalake = datalake;
        this.downloader = downloader;
        this.metadataFetcher = metadataFetcher;
        this.replicationManager = replicationManager;
        this.brokerPublisher = brokerPublisher;
    }

    private void copyToLocalBackup(String documentId) {
        try {
            log.info("Copying document {} to local backup...", documentId);

            Path source = Path.of("/data/datalake/docs/" + documentId);
            Path target = Path.of("/local-backup/docs/" + documentId);

            Files.createDirectories(target);

            Files.walk(source).forEach(path -> {
                try {
                    Path relative = source.relativize(path);
                    Path dest = target.resolve(relative);

                    if (Files.isDirectory(path)) {
                        Files.createDirectories(dest);
                    } else {
                        Files.copy(path, dest, StandardCopyOption.REPLACE_EXISTING);
                    }
                } catch (Exception e) {
                    log.error("Error copying file {}: {}", path, e.getMessage());
                }
            });

            log.info("Local backup completed for {}", documentId);

        } catch (Exception e) {
            log.error("Error copying to local backup for {}: {}", documentId, e.getMessage());
        }
    }

    public void ingest(String documentId) {
        if (statusMap.get(documentId) == IngestionStatus.COMPLETED) {
            log.info("Skipping ingestion for {} (already completed)", documentId);
            return;
        }

        try {
            log.info("Starting ingestion for {}", documentId);
            statusMap.put(documentId, IngestionStatus.DOWNLOADING);

            log.info("Downloading document {}", documentId);
            var dl = downloader.download(documentId);

            int idNum = Integer.parseInt(documentId);
            MetadataFetcher.Metadata metaInfo = metadataFetcher.fetch(idNum);

            Map<String, Object> metadata = new LinkedHashMap<>();
            metadata.put("id", documentId);
            metadata.put("title", metaInfo.title);
            metadata.put("author", metaInfo.author);
            metadata.put("language", metaInfo.language);
            metadata.put("releaseDate", metaInfo.releaseDate);
            metadata.put("sourceUrl", dl.sourceUrl);
            metadata.put("timestamp", ZonedDateTime.now(ZoneId.systemDefault()).toString());

            log.info("Storing document {} in datalake", documentId);
            statusMap.put(documentId, IngestionStatus.STORING);

            Path localPath = datalake.storeDocument(
                    documentId,
                    dl.header,
                    dl.body,
                    dl.sourceUrl,
                    metaInfo.title,
                    metaInfo.author,
                    metaInfo.language,
                    metaInfo.releaseDate
            );

            log.info("Creating local backup for {}", documentId);
            copyToLocalBackup(documentId);

            log.info("Replicating document {}", documentId);
            statusMap.put(documentId, IngestionStatus.REPLICATING);
            replicationManager.replicate(documentId, dl.header, dl.body, dl.sourceUrl, metadata);

            log.info("Publishing ingestion event for {}", documentId);
            statusMap.put(documentId, IngestionStatus.PUBLISHING_EVENT);
            brokerPublisher.publishDocumentIngested(documentId, localPath.toString(), dl.sourceUrl);

            statusMap.put(documentId, IngestionStatus.COMPLETED);
            log.info("Completed ingestion for {}", documentId);

        } catch (Exception e) {
            statusMap.put(documentId, IngestionStatus.FAILED);
            log.error("Ingestion FAILED for {}: {}", documentId, e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public void storeReplicaFromJson(String documentId, String jsonPayload) {
        try {
            log.info("Storing replica for {}", documentId);

            JsonObject obj = JsonParser.parseString(jsonPayload).getAsJsonObject();
            String header = obj.has("header") ? obj.get("header").getAsString() : "";
            String body = obj.has("body") ? obj.get("body").getAsString() : "";
            String sourceUrl = obj.has("sourceUrl") ? obj.get("sourceUrl").getAsString() : "";

            Map<String, Object> metadata = null;
            if (obj.has("metadata") && obj.get("metadata").isJsonObject()) {
                metadata = gson.fromJson(obj.get("metadata"), Map.class);
            }

            datalake.storeReplica(documentId, header, body, sourceUrl, metadata);

            log.info("Replica stored for {}", documentId);

        } catch (Exception e) {
            log.error("Failed to store replica for {}: {}", documentId, e.getMessage());
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
