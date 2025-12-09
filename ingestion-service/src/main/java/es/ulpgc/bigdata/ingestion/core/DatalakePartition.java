package es.ulpgc.bigdata.ingestion.core;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents the local partition of the distributed datalake.
 *
 * The Stage-3 architecture requires each ingestion node to store its documents
 * locally. Replication ensures each document appears in R partitions.
 *
 * The indexer will later read from these files (using the path sent in the
 * message).
 */
public class DatalakePartition {

    private static final Logger log = LoggerFactory.getLogger(DatalakePartition.class);

    private final Path rootDir;

    public DatalakePartition(Path rootDir) {
        this.rootDir = rootDir;
    }

    /**
     * Saves a document on disk under: datalake/docs/{documentId}.txt
     */
    public Path storeDocument(String documentId, String content) throws IOException {
        Path docDir = rootDir.resolve("docs");
        Files.createDirectories(docDir);

        Path path = docDir.resolve(documentId + ".txt");

        // Write the document content to disk
        Files.writeString(path, content, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        log.info("Stored document {} at {}", documentId, path.toAbsolutePath());
        return path;
    }

    /**
     * Returns all documents stored locally in this datalake partition.
     */
    public Map<String, Path> listDocuments() {
        Path docDir = rootDir.resolve("docs");
        if (!Files.exists(docDir)) {
            return Collections.emptyMap();
        }

        Map<String, Path> result = new HashMap<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(docDir, "*.txt")) {
            for (Path p : stream) {
                String fileName = p.getFileName().toString();
                String id = fileName.substring(0, fileName.length() - 4);  // remove .txt
                result.put(id, p);
            }
        } catch (IOException e) {
            log.error("Error listing documents: {}", e.getMessage(), e);
        }
        return result;
    }
}
