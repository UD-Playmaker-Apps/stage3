package es.ulpgc.bigdata.ingestion;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import es.ulpgc.bigdata.ingestion.api.IngestionController;
import es.ulpgc.bigdata.ingestion.core.BrokerPublisher;
import es.ulpgc.bigdata.ingestion.core.DatalakePartition;
import es.ulpgc.bigdata.ingestion.core.DocumentDownloader;
import es.ulpgc.bigdata.ingestion.core.IngestionService;
import es.ulpgc.bigdata.ingestion.core.ReplicationManager;
import io.javalin.Javalin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestionApplication {

    private static final Logger log = LoggerFactory.getLogger(IngestionApplication.class);

    public static void main(String[] args) {

        int port = Integer.parseInt(System.getenv().getOrDefault("INGESTION_PORT", "7001"));
        String datalakeDir = System.getenv().getOrDefault("DATALAKE_DIR", "./datalake");
        int replicationFactor = Integer.parseInt(System.getenv().getOrDefault("REPLICATION_FACTOR", "2"));
        String peersEnv = System.getenv().getOrDefault("INGESTION_PEERS", "");
        String brokerUrl = System.getenv().getOrDefault("BROKER_URL", "tcp://activemq:61616");
        String queueName = System.getenv().getOrDefault("BROKER_QUEUE_INGESTED", "document.ingested");

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

        DatalakePartition datalake = new DatalakePartition(Path.of(datalakeDir));
        DocumentDownloader downloader = new DocumentDownloader();
        ReplicationManager replicationManager = new ReplicationManager(peers, replicationFactor);
        BrokerPublisher brokerPublisher = new BrokerPublisher(brokerUrl, queueName);

        IngestionService ingestionService = new IngestionService(
                datalake, downloader, replicationManager, brokerPublisher
        );

        Javalin app = Javalin.create(config -> {
            config.showJavalinBanner = false;
        });

        new IngestionController(app, ingestionService).registerRoutes();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                brokerPublisher.close();
            } catch (Exception ignored) {}
        }));
        app.start(port);
    }
}
