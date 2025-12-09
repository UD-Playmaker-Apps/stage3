package es.ulpgc.bigdata.ingestion.core;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.google.gson.Gson;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents the local partition of the distributed datalake.
 *
 * Stores each document as:
 *   datalake/docs/<documentId>/header.txt
 *   datalake/docs/<documentId>/body.txt
 *
 * Also keeps a small ingestion log: ingestion-log.json
 */
public class DatalakePartition {

    private static final Logger log = LoggerFactory.getLogger(DatalakePartition.class);

    private final Path rootDir;
    private final Path docsDir;
    private final Path logFile;
    private final Gson gson = new Gson();

    public DatalakePartition(Path rootDir) {
        this.rootDir = rootDir.toAbsolutePath().normalize();
        this.docsDir = this.rootDir.resolve("docs");
        this.logFile = this.rootDir.resolve("ingestion-log.json");
        try {
            Files.createDirectories(docsDir);
            if (!Files.exists(logFile)) {
                Files.writeString(logFile, "{}", StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize datalake dir: " + e.getMessage(), e);
        }
    }

    /**
     * Store header and body under datalake/docs/<documentId>/header.txt & body.txt
     * Returns the Path to the document folder.
     */
    public Path storeDocument(String documentId, String header, String body, String sourceUrl) throws IOException {
        Path docDir = docsDir.resolve(documentId);
        Files.createDirectories(docDir);

        Path headerFile = docDir.resolve("header.txt");
        Path bodyFile = docDir.resolve("body.txt");

        Files.writeString(headerFile, header == null ? "" : header, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        Files.writeString(bodyFile, body == null ? "" : body, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        // Update ingestion log
        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("path", docDir.toString());
        meta.put("sourceUrl", sourceUrl);
        meta.put("timestamp", ZonedDateTime.now(ZoneId.systemDefault()).toString());
        updateLog(documentId, meta);

        log.info("Stored document {} at {}", documentId, docDir.toAbsolutePath());
        return docDir;
    }

    /**
     * Store replica by accepting header/body; same as storeDocument but does not publish events.
     */
    public Path storeReplica(String documentId, String header, String body, String sourceUrl) throws IOException {
        return storeDocument(documentId, header, body, sourceUrl);
    }

    public Map<String, Path> listDocuments() {
        if (!Files.exists(docsDir)) {
            return Collections.emptyMap();
        }

        Map<String, Path> result = new HashMap<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(docsDir)) {
            for (Path p : stream) {
                if (Files.isDirectory(p)) {
                    String id = p.getFileName().toString();
                    result.put(id, p);
                }
            }
        } catch (IOException e) {
            log.error("Error listing documents: {}", e.getMessage(), e);
        }
        return result;
    }

    private synchronized void updateLog(String documentId, Map<String, Object> meta) {
        try {
            String raw = Files.readString(logFile, StandardCharsets.UTF_8);
            Map<String, Object> db = gson.fromJson(raw, Map.class);
            if (db == null) db = new LinkedHashMap<>();
            db.put(documentId, meta);
            String out = gson.toJson(db);
            Files.writeString(logFile, out, StandardCharsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            log.error("Failed to update ingestion log: {}", e.getMessage(), e);
        }
    }
}
