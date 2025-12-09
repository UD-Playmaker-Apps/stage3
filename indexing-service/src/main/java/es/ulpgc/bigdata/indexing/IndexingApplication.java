
package es.ulpgc.bigdata.indexing;

import es.ulpgc.bigdata.indexing.api.IndexingStatusController;
import es.ulpgc.bigdata.indexing.index.HazelcastIndexProvider;
import es.ulpgc.bigdata.indexing.messaging.JmsIndexingConsumer;
import io.javalin.Javalin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * Entry point del servicio de indexación:
 * - Arranca Hazelcast (miembro) y obtiene la MultiMap "inverted-index".
 * - Crea el consumidor JMS para la cola de "document.ingested".
 * - Expone /health y /index/status vía Javalin.
 */
public class IndexingApplication {
    private static final Logger log = LoggerFactory.getLogger(IndexingApplication.class);

    public static void main(String[] args) throws Exception {
        // Configuración del cluster Hazelcast
        String clusterName = System.getenv().getOrDefault("HZ_CLUSTER_NAME", "search-cluster");
        int backupCount    = Integer.parseInt(System.getenv().getOrDefault("HZ_BACKUP_COUNT", "2"));
        int asyncBackup    = Integer.parseInt(System.getenv().getOrDefault("HZ_ASYNC_BACKUP_COUNT", "1"));

        // Inicializar proveedor de índice (Hazelcast)
        HazelcastIndexProvider indexProvider = new HazelcastIndexProvider(clusterName, backupCount, asyncBackup);

        // Config JMS/ActiveMQ
        String brokerUrl = System.getenv().getOrDefault("BROKER_URL", "tcp://activemq:61616");
        String queueName = System.getenv().getOrDefault("BROKER_QUEUE_INGESTED", "document.ingested");

        // Consumidor JMS
        ConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
        Connection connection = factory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination queue = session.createQueue(queueName);

        JmsIndexingConsumer consumerLogic = new JmsIndexingConsumer(indexProvider);
        MessageConsumer consumer = session.createConsumer(queue);
        consumer.setMessageListener(consumerLogic);

        // Web API de estado
        int port = Integer.parseInt(System.getenv().getOrDefault("INDEXING_PORT", "7002"));
        Javalin app = Javalin.create(cfg -> cfg.showJavalinBanner = false);
        new IndexingStatusController(app, indexProvider).registerRoutes();
        app.start(port);

        log.info("Indexing Service started on port {}", port);
        log.info("Broker: {}, queue: {}", brokerUrl, queueName);

        // Shutdown hook para liberar recursos
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                consumer.close();
                session.close();
                connection.close();
            } catch (Exception ignore) {}
            indexProvider.shutdown();
            app.stop();
        }));
    }
}
