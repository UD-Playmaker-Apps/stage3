package es.ulpgc.bigdata.ingestion.api;

import java.nio.file.Path;
import java.util.Map;

import es.ulpgc.bigdata.ingestion.api.dto.DocumentInfoResponse;
import es.ulpgc.bigdata.ingestion.api.dto.IngestionStatusResponse;
import es.ulpgc.bigdata.ingestion.core.IngestionService;
import es.ulpgc.bigdata.ingestion.core.IngestionStatus;
import io.javalin.Javalin;
import io.javalin.http.Context;

/**
 * Exposes REST endpoints for the ingestion service.
 *
 * Clients (or the control module) call: - POST /ingest/{id} : start ingestion
 * for a document - GET /ingest/status/{id} : check ingestion progress - GET
 * /ingest/list : list documents stored locally
 *
 * Internal endpoint: - POST /internal/replica/{id} : receives replication from
 * peers
 */
public class IngestionController {

    private final Javalin app;
    private final IngestionService ingestionService;

    public IngestionController(Javalin app, IngestionService ingestionService) {
        this.app = app;
        this.ingestionService = ingestionService;
    }

    public void registerRoutes() {

        // Start the ingestion pipeline for the given document
        app.post("/ingest/{id}", this::startIngestion);

        // Query ingestion status (useful for debugging)
        app.get("/ingest/status/{id}", this::getStatus);

        // Lists locally stored documents
        app.get("/ingest/list", this::listDocuments);

        // Internal-only endpoint used for replication
        app.post("/internal/replica/{id}", this::receiveReplica);

        // Optional health endpoint (recommended for Docker)
        app.get("/health", ctx -> ctx.result("OK"));
    }

    private void startIngestion(Context ctx) {
        String id = ctx.pathParam("id");
        ingestionService.ingest(id);
        ctx.status(202).result("Ingestion started for " + id);
    }

    private void getStatus(Context ctx) {
        String id = ctx.pathParam("id");
        IngestionStatus status = ingestionService.getStatus(id);
        ctx.json(new IngestionStatusResponse(id, status.name()));
    }

    private void listDocuments(Context ctx) {
        Map<String, Path> docs = ingestionService.listDocuments();
        DocumentInfoResponse[] resp = docs.entrySet().stream()
                .map(e -> new DocumentInfoResponse(e.getKey(), e.getValue().toString()))
                .toArray(DocumentInfoResponse[]::new);
        ctx.json(resp);
    }

    /**
     * Receives replicated content from other ingestion nodes. IMPORTANT:
     * replicas must NOT republish ingestion events.
     */
    private void receiveReplica(Context ctx) {
        String id = ctx.pathParam("id");
        String content = ctx.body();
        ingestionService.storeReplica(id, content);
        ctx.status(201).result("Replica stored for " + id);
    }
}
