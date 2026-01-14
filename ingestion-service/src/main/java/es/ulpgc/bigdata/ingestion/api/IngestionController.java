package es.ulpgc.bigdata.ingestion.api;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import es.ulpgc.bigdata.ingestion.api.dto.DocumentInfoResponse;
import es.ulpgc.bigdata.ingestion.api.dto.IngestionStatusResponse;
import es.ulpgc.bigdata.ingestion.core.IngestionService;
import es.ulpgc.bigdata.ingestion.core.IngestionStatus;

import io.javalin.Javalin;
import io.javalin.http.Context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestionController {

    private static final Logger log = LoggerFactory.getLogger(IngestionController.class);

    private final Javalin app;
    private final IngestionService ingestionService;

    public IngestionController(Javalin app, IngestionService ingestionService) {
        this.app = app;
        this.ingestionService = ingestionService;
    }

    public void registerRoutes() {
        app.post("/ingest/{id}", this::startIngestion);
        app.get("/ingest/status/{id}", this::getStatus);
        app.get("/ingest/list", this::listDocuments);
        app.post("/internal/replica/{id}", this::receiveReplica);
        app.get("/health", ctx -> ctx.result("OK"));
    }

    private void startIngestion(Context ctx) {
        String id = ctx.pathParam("id");

        // 1. Comprobar si ya existe en el datalake persistente
        Path docDir = Path.of("/data/datalake/docs/" + id);
        if (Files.exists(docDir) && Files.isDirectory(docDir)) {
            ctx.status(409).result("Document already ingested: " + id);
            return;
        }

        // 2. Comprobar si ya está marcado como COMPLETED en memoria
        IngestionStatus status = ingestionService.getStatus(id);
        if (status == IngestionStatus.COMPLETED) {
            ctx.status(409).result("Document already ingested: " + id);
            return;
        }

        // 3. Lanzar la ingesta en un hilo separado
        new Thread(() -> {
            try {
                ingestionService.ingest(id);
            } catch (Exception e) {
                log.error("Ingestion thread error for {}: {}", id, e.getMessage(), e);
            }
        }).start();

        ctx.status(202).result("Ingestion started for " + id);
    }

    private void getStatus(Context ctx) {
        String id = ctx.pathParam("id");
        IngestionStatus status = ingestionService.getStatus(id);
        ctx.json(new IngestionStatusResponse(id, status.name()));
    }

    private void listDocuments(Context ctx) {
        Map<String, ?> docs = ingestionService.listDocuments();
        DocumentInfoResponse[] resp = docs.entrySet().stream()
                .map(e -> new DocumentInfoResponse(e.getKey(), e.getValue().toString()))
                .toArray(DocumentInfoResponse[]::new);
        ctx.json(resp);
    }

    private void receiveReplica(Context ctx) {
        String id = ctx.pathParam("id");
        String body = ctx.body();
        try {
            ingestionService.storeReplicaFromJson(id, body);
            ctx.status(201).result("Replica stored for " + id);
        } catch (Exception e) {
            log.error("Error storing replica {}: {}", id, e.getMessage(), e);
            ctx.status(500).result("Failed to store replica: " + e.getMessage());
        }
    }
}