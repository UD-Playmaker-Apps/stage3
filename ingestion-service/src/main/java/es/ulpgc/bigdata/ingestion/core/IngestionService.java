package es.ulpgc.bigdata.ingestion.core;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.nio.file.Path;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class IngestionService {

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

    public void ingest(String documentId) {
        if (statusMap.get(documentId) == IngestionStatus.COMPLETED) return;

        statusMap.put(documentId, IngestionStatus.DOWNLOADING);
        try {
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

            statusMap.put(documentId, IngestionStatus.REPLICATING);
            replicationManager.replicate(documentId, dl.header, dl.body, dl.sourceUrl, metadata);

            statusMap.put(documentId, IngestionStatus.PUBLISHING_EVENT);
            brokerPublisher.publishDocumentIngested(documentId, localPath.toString(), dl.sourceUrl);

            statusMap.put(documentId, IngestionStatus.COMPLETED);

        } catch (Exception e) {
            statusMap.put(documentId, IngestionStatus.FAILED);
            throw new RuntimeException(e);
        }
    }

    public void storeReplicaFromJson(String documentId, String jsonPayload) {
        try {
            JsonObject obj = JsonParser.parseString(jsonPayload).getAsJsonObject();
            String header = obj.has("header") ? obj.get("header").getAsString() : "";
            String body = obj.has("body") ? obj.get("body").getAsString() : "";
            String sourceUrl = obj.has("sourceUrl") ? obj.get("sourceUrl").getAsString() : "";

            Map<String, Object> metadata = null;
            if (obj.has("metadata") && obj.get("metadata").isJsonObject()) {
                metadata = gson.fromJson(obj.get("metadata"), Map.class);
            }

            datalake.storeReplica(documentId, header, body, sourceUrl, metadata);
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