package es.ulpgc.bigdata.ingestion;

import es.ulpgc.bigdata.ingestion.api.IngestionController;
import es.ulpgc.bigdata.ingestion.core.*;
import io.javalin.Javalin;
import io.javalin.json.JavalinJackson;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

public class IngestionApplication {

    public static void main(String[] args) {

        String datalakeDir = System.getenv().getOrDefault("DATALAKE_DIR", "/data/datalake");
        String peersEnv = System.getenv().getOrDefault("INGESTION_PEERS", "");
        int replicationFactor = Integer.parseInt(System.getenv().getOrDefault("REPLICATION_FACTOR", "2"));
        String brokerUrl = System.getenv().getOrDefault("BROKER_URL", "tcp://activemq:61616");
        String queueName = System.getenv().getOrDefault("BROKER_QUEUE", "document.ingested");
        int port = Integer.parseInt(System.getenv().getOrDefault("INGESTION_PORT", "7001"));

        List<String> peers = peersEnv.isBlank()
                ? List.of()
                : Arrays.stream(peersEnv.split(",")).map(String::trim).toList();

        DatalakePartition datalake = new DatalakePartition(Path.of(datalakeDir));
        DocumentDownloader downloader = new DocumentDownloader();
        MetadataFetcher metadataFetcher = new MetadataFetcher();
        ReplicationManager replicationManager = new ReplicationManager(peers, replicationFactor);
        BrokerPublisher brokerPublisher = new BrokerPublisher(brokerUrl, queueName);

        IngestionService ingestionService =
                new IngestionService(datalake, downloader, metadataFetcher, replicationManager, brokerPublisher);

        // ✅ JSON mapper correcto para Javalin 5
        Javalin app = Javalin.create(config -> {
            config.jsonMapper(new JavalinJackson());
        });

        new IngestionController(app, ingestionService).registerRoutes();

        app.get("/ingest/raw/{id}", ctx -> {
            var doc = datalake.readDocumentWithMetadata(ctx.pathParam("id"));
            if (doc == null) ctx.status(404).result("Not found");
            else ctx.json(doc);
        });

        app.start(port);
    }
}
