package es.ulpgc.bigdata.indexing.messaging;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import es.ulpgc.bigdata.indexing.api.dto.DocumentContent;
import es.ulpgc.bigdata.indexing.index.HazelcastIndexProvider;
import es.ulpgc.bigdata.indexing.util.TextTokenizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

public class JmsIndexingConsumer implements MessageListener {

    private static final Logger log = LoggerFactory.getLogger(JmsIndexingConsumer.class);
    private final HazelcastIndexProvider indexProvider;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
    private final String ingestionBase = System.getenv().getOrDefault("INGESTION_BASE", "http://ingestion1:7001");
    private final Gson gson = new Gson();

    public JmsIndexingConsumer(HazelcastIndexProvider indexProvider) {
        this.indexProvider = indexProvider;
    }

    @Override
    public void onMessage(Message message) {
        try {
            if (!(message instanceof TextMessage tm)) {
                log.warn("Unsupported JMS message type: {}", message.getClass());
                return;
            }

            JsonObject ev = JsonParser.parseString(tm.getText()).getAsJsonObject();
            String id = ev.has("documentId") && !ev.get("documentId").isJsonNull()
                    ? ev.get("documentId").getAsString() : null;
            String path = ev.has("path") && !ev.get("path").isJsonNull()
                    ? ev.get("path").getAsString() : null;

            if (id == null || id.isBlank()) {
                log.warn("Received event without documentId: {}", ev);
                return;
            }

            if (indexProvider.indexedDocs().contains(id)) {
                log.info("Skipping already-indexed {}", id);
                return;
            }

            DocumentContent doc = tryReadLocal(path);
            if (doc == null || doc.body == null || doc.body.isBlank()) {
                doc = fetchFromIngestion(id);
            }

            if (doc == null || doc.body == null || doc.body.isBlank()) {
                log.warn("Empty content for {}, skipping", id);
                return;
            }

            for (String term : TextTokenizer.tokens(doc.body)) {
                indexProvider.invertedIndex().put(term, id);
            }

            if (doc.metadata != null) {
                indexProvider.metadataIndex().put(id, doc.metadata);
            }

            indexProvider.indexedDocs().add(id);
            log.info("Indexed {}", id);

        } catch (Exception e) {
            log.error("Indexing failed: {}", e.getMessage(), e);
        }
    }

    private DocumentContent tryReadLocal(String path) {
        if (path == null || path.isBlank()) return null;
        try {
            Path p = Path.of(path);
            if (!Files.isDirectory(p)) return null;

            DocumentContent doc = new DocumentContent();

            Path header = p.resolve("header.txt");
            Path body = p.resolve("body.txt");
            Path metadata = p.resolve("metadata.json");

            doc.header = Files.exists(header)
                    ? Files.readString(header, StandardCharsets.UTF_8) : "";
            doc.body = Files.exists(body)
                    ? Files.readString(body, StandardCharsets.UTF_8) : "";

            if (Files.exists(metadata)) {
                String raw = Files.readString(metadata, StandardCharsets.UTF_8);
                doc.metadata = gson.fromJson(raw, java.util.Map.class);
            }

            return doc;
        } catch (Exception e) {
            log.warn("Local read failed for {}: {}", path, e.getMessage());
            return null;
        }
    }

    private DocumentContent fetchFromIngestion(String id) {
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(ingestionBase + "/ingest/raw/" + id))
                    .timeout(Duration.ofSeconds(5))
                    .GET()
                    .build();

            HttpResponse<String> res = http.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (res.statusCode() != 200 || res.body() == null || res.body().isBlank()) {
                log.warn("Ingestion returned {} when fetching {}", res.statusCode(), id);
                return null;
            }

            return gson.fromJson(res.body(), DocumentContent.class);

        } catch (Exception e) {
            log.error("Error fetching {} from ingestion: {}", id, e.getMessage());
            return null;
        }
    }
}
