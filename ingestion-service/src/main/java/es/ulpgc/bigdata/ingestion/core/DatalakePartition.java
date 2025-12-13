package es.ulpgc.bigdata.ingestion.core;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;

public class DatalakePartition {

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

    public Path storeDocument(String documentId, String header, String body, String sourceUrl,
                              String title, String author, String language, String releaseDate) throws IOException {

        Path docDir = docsDir.resolve(documentId);
        Files.createDirectories(docDir);

        Path headerFile = docDir.resolve("header.txt");
        Path bodyFile = docDir.resolve("body.txt");
        Path metadataFile = docDir.resolve("metadata.json");

        Files.writeString(headerFile, header == null ? "" : header, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        Files.writeString(bodyFile, body == null ? "" : body, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("id", documentId);
        meta.put("title", title);
        meta.put("author", author);
        meta.put("language", language);
        meta.put("releaseDate", releaseDate);
        meta.put("sourceUrl", sourceUrl);
        meta.put("timestamp", ZonedDateTime.now(ZoneId.systemDefault()).toString());
        meta.put("path", docDir.toString());

        Files.writeString(metadataFile, gson.toJson(meta), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        updateLog(documentId, meta);

        return docDir;
    }

    public Path storeReplica(String documentId, String header, String body, String sourceUrl,
                             Map<String, Object> incomingMetadata) throws IOException {

        Path docDir = docsDir.resolve(documentId);
        Files.createDirectories(docDir);

        Path headerFile = docDir.resolve("header.txt");
        Path bodyFile = docDir.resolve("body.txt");
        Path metadataFile = docDir.resolve("metadata.json");

        Files.writeString(headerFile, header == null ? "" : header, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        Files.writeString(bodyFile, body == null ? "" : body, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        Map<String, Object> meta = new LinkedHashMap<>();
        if (incomingMetadata != null && !incomingMetadata.isEmpty()) {
            meta.putAll(incomingMetadata);
        }
        meta.put("path", docDir.toString());
        if (!meta.containsKey("id")) meta.put("id", documentId);
        if (!meta.containsKey("timestamp")) {
            meta.put("timestamp", ZonedDateTime.now(ZoneId.systemDefault()).toString());
        }
        if (!meta.containsKey("sourceUrl")) meta.put("sourceUrl", sourceUrl == null ? "" : sourceUrl);

        Files.writeString(metadataFile, gson.toJson(meta), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        updateLog(documentId, meta);

        return docDir;
    }

    public Map<String, Path> listDocuments() {
        if (!Files.exists(docsDir)) return Collections.emptyMap();
        Map<String, Path> result = new HashMap<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(docsDir)) {
            for (Path p : stream) {
                if (Files.isDirectory(p)) {
                    result.put(p.getFileName().toString(), p);
                }
            }
        } catch (IOException ignored) {}
        return result;
    }

    public Map<String, Object> readDocumentWithMetadata(String documentId) {
        Path docDir = docsDir.resolve(documentId);
        if (!Files.exists(docDir) || !Files.isDirectory(docDir)) return null;

        try {
            Path bodyFile = docDir.resolve("body.txt");
            Path metadataFile = docDir.resolve("metadata.json");

            String body = Files.exists(bodyFile)
                    ? Files.readString(bodyFile, StandardCharsets.UTF_8)
                    : "";

            Map<String, Object> meta = new LinkedHashMap<>();
            if (Files.exists(metadataFile)) {
                String raw = Files.readString(metadataFile, StandardCharsets.UTF_8);
                Type type = new TypeToken<Map<String, Object>>(){}.getType();
                Map<String, Object> parsed = gson.fromJson(raw, type);
                if (parsed != null) meta.putAll(parsed);
            }

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("metadata", meta);
            result.put("body", body);
            return result;

        } catch (IOException e) {
            return null;
        }
    }

    private synchronized void updateLog(String documentId, Map<String, Object> meta) {
        try {
            String raw = Files.readString(logFile, StandardCharsets.UTF_8);
            Type type = new TypeToken<Map<String, Object>>(){}.getType();
            Map<String, Object> db = gson.fromJson(raw, type);
            if (db == null) db = new LinkedHashMap<>();
            db.put(documentId, meta);
            Files.writeString(logFile, gson.toJson(db), StandardCharsets.UTF_8,
                    StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException ignored) {}
    }
}