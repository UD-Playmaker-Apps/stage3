package es.ulpgc.bigdata.ingestion;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import es.ulpgc.bigdata.ingestion.api.IngestionController;
import es.ulpgc.bigdata.ingestion.core.BrokerPublisher;
import es.ulpgc.bigdata.ingestion.core.DatalakePartition;
import es.ulpgc.bigdata.ingestion.core.DocumentDownloader;
import es.ulpgc.bigdata.ingestion.core.IngestionService;
import es.ulpgc.bigdata.ingestion.core.ReplicationManager;
import io.javalin.Javalin;

/**
 * Entry point for the Ingestion Service.
 *
 * This service is responsible for: - Downloading documents (crawler role) -
 * Storing them in a local datalake partition - Replicating these documents to
 * other ingestion nodes - Publishing ingestion events to the Message Broker
 * (ActiveMQ)
 *
 * It forms the first stage of the Stage-3 distributed architecture.
 */
public class IngestionApplication {

    private static final Logger log = LoggerFactory.getLogger(IngestionApplication.class);

    public static void main(String[] args) {

        // The port where this ingestion-service instance will run
        int port = Integer.parseInt(System.getenv().getOrDefault("INGESTION_PORT", "7001"));

        // Directory where this node's datalake partition will store documents
        String datalakeDir = System.getenv().getOrDefault("DATALAKE_DIR", "./datalake");

        // Replication factor (R). Each document is stored in R different ingestion nodes.
        int replicationFactor = Integer.parseInt(System.getenv().getOrDefault("REPLICATION_FACTOR", "2"));

        // Comma-separated list of peer ingestion nodes, used for replication
        String peersEnv = System.getenv().getOrDefault("INGESTION_PEERS", "");

        // ActiveMQ broker connection URL
        String brokerUrl = System.getenv().getOrDefault("BROKER_URL", "tcp://activemq:61616");

        // Name of the queue where ingestion events will be published
        String queueName = System.getenv().getOrDefault("BROKER_QUEUE_INGESTED", "document.ingested");

        // Transform comma-separated peers into a List<String>
        List<String> peers = peersEnv.isBlank()
                ? List.of()
                : Arrays.stream(peersEnv.split(","))
                        .map(String::trim)
                        .filter(s -> !s.isEmpty())
                        .toList();

        log.info("Starting Ingestion Service on port {}", port);
        log.info("Datalake dir: {}", datalakeDir);
        log.info("Replication factor R = {}", replicationFactor);
        log.info("Peers: {}", peers);
        log.info("Broker URL: {}, queue: {}", brokerUrl, queueName);

        // Local datalake partition for storing documents
        DatalakePartition datalake = new DatalakePartition(Path.of(datalakeDir));

        // Downloads the document content (mocked for now)
        DocumentDownloader downloader = new DocumentDownloader();

        // Handles replication to other ingestion nodes
        ReplicationManager replicationManager = new ReplicationManager(peers, replicationFactor);

        // Publishes ingestion events to ActiveMQ
        BrokerPublisher brokerPublisher = new BrokerPublisher(brokerUrl, queueName);

        // Core logic that orchestrates downloading, replication, and event publishing
        IngestionService ingestionService = new IngestionService(
                datalake, downloader, replicationManager, brokerPublisher
        );

        // Web API built with Javalin, exposing /ingest, /status, /list, /internal/replica
        Javalin app = Javalin.create(config -> {
            config.showJavalinBanner = false;
        });

        new IngestionController(app, ingestionService).registerRoutes();
        app.start(port);
    }
}
