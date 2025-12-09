package es.ulpgc.bigdata.ingestion.core;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
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
 */
public class BrokerPublisher implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(BrokerPublisher.class);

    private final Connection connection;
    private final Session session;
    private final MessageProducer producer;

    public BrokerPublisher(String brokerUrl, String queueName) {
        try {
            ConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
            this.connection = factory.createConnection();
            this.connection.start();
            this.session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination queue = session.createQueue(queueName);
            this.producer = session.createProducer(queue);
        } catch (JMSException e) {
            throw new RuntimeException("Failed to create BrokerPublisher", e);
        }
    }

    public void publishDocumentIngested(String documentId, String localPath, String sourceUrl) {
        try {
            JsonObject payload = new JsonObject();
            payload.addProperty("documentId", documentId);
            payload.addProperty("path", localPath);
            payload.addProperty("sourceUrl", sourceUrl);
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
        try {
            if (producer != null) producer.close();
        } catch (JMSException ignored) {}
        try {
            if (session != null) session.close();
        } catch (JMSException ignored) {}
        try {
            if (connection != null) connection.close();
        } catch (JMSException ignored) {}
    }
}
