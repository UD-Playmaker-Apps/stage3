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

/**
 * Consumidor JMS que procesa eventos "document.ingested":
 * - Intenta leer el contenido desde path local.
 * - Si falla, hace fallback a GET /ingest/raw/{id} del Ingestion Service.
 * - Tokeniza y actualiza la MultiMap "inverted-index".
 */
public class JmsIndexingConsumer implements MessageListener {
    private static final Logger log = LoggerFactory.getLogger(JmsIndexingConsumer.class);

    private final HazelcastIndexProvider indexProvider;
    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3))
            .build();
    private final String ingestionBase =
            System.getenv().getOrDefault("INGESTION_BASE", "http://ingestion1:7001");

    public JmsIndexingConsumer(HazelcastIndexProvider indexProvider) {
        this.indexProvider = indexProvider;
    }

    @Override
    public void onMessage(Message message) {
        try {
            if (message instanceof TextMessage tm) {
                JsonObject ev = JsonParser.parseString(tm.getText()).getAsJsonObject();
                String id = ev.get("documentId").getAsString();
                String path = ev.has("path") ? ev.get("path").getAsString() : null;

                String content = tryReadLocal(path);
                if (content == null) content = fetchFromIngestion(id);

                for (String term : TextTokenizer.tokens(content)) {
                    if (!indexProvider.invertedIndex().containsEntry(term, id)) {
                        indexProvider.invertedIndex().put(term, id);
                    }
                }
                log.info("Indexed {}", id);
            }
        } catch (Exception e) {
            log.error("Indexing failed: {}", e.getMessage(), e);
        }
    }

    private String tryReadLocal(String path) {
        if (path == null) return null;
        try {
            return Files.readString(Path.of(path, "body.txt"), StandardCharsets.UTF_8);
        } catch (Exception e) {
            log.warn("Local path not readable: {} ({})", path, e.getMessage());
            return null;
        }
    }

    private String fetchFromIngestion(String id) {
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(ingestionBase + "/ingest/raw/" + id))
                    .timeout(Duration.ofSeconds(5))
                    .build();
            HttpResponse<String> res = http.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (res.statusCode() == 200) {
                DocumentContent doc = new Gson().fromJson(res.body(), DocumentContent.class);
                return doc.header + "\n" + doc.body;
            }
        } catch (Exception e) {
            log.error("Error fetching {} from ingestion: {}", id, e.getMessage());
        }
        throw new RuntimeException("Cannot load content for " + id);
    }
}
