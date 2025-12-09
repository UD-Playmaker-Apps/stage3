package es.ulpgc.bigdata.ingestion.core;

import javax.jms.Connection;
import javax.jms.ConnectionFactory; // ActiveMQ 5.x uses javax.jms, not jakarta.jms
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;

/**
 * Publishes ingestion events to ActiveMQ.
 *
 * This is what enables the event-driven, decoupled architecture: Ingestion -->
 * Broker --> Indexers
 *
 * Indexers will listen for "document.ingested" events and process the new
 * documents asynchronously.
 */
public class BrokerPublisher implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(BrokerPublisher.class);

    private final Connection connection;
    private final Session session;
    private final MessageProducer producer;

    public BrokerPublisher(String brokerUrl, String queueName) {
        try {
            // Create a JMS connection to ActiveMQ
            ConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
            this.connection = factory.createConnection();
            this.connection.start();

            // Create a session with automatic acknowledgement
            this.session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Destination = queue where messages will be sent
            Destination queue = session.createQueue(queueName);

            // Producer that sends messages into the queue
            this.producer = session.createProducer(queue);

        } catch (JMSException e) {
            throw new RuntimeException("Failed to create BrokerPublisher", e);
        }
    }

    /**
     * Publishes the "DOCUMENT_INGESTED" event to ActiveMQ.
     *
     * The event includes: - documentId - local path to the document in datalake
     * - timestamp
     *
     * Indexers will consume this event and update the distributed in-memory
     * inverted index.
     */
    public void publishDocumentIngested(String documentId, String localPath) {
        try {
            JsonObject payload = new JsonObject();
            payload.addProperty("documentId", documentId);
            payload.addProperty("path", localPath);
            payload.addProperty("eventType", "DOCUMENT_INGESTED");
            payload.addProperty("timestamp", System.currentTimeMillis());

            TextMessage message = session.createTextMessage(payload.toString());
            producer.send(message);

            log.info("Published DOCUMENT_INGESTED for {}", documentId);

        } catch (JMSException e) {
            log.error("Error publishing event for {}: {}", documentId, e.getMessage(), e);
        }
    }

    @Override
    public void close() throws Exception {
        producer.close();
        session.close();
        connection.close();
    }
}
