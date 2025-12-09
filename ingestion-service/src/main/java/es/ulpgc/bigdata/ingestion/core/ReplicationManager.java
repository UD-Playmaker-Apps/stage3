package es.ulpgc.bigdata.ingestion.core;

import java.util.List;

import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles replication of documents between ingestion nodes.
 *
 * If replicationFactor = R, then for each document we send R-1 replicas to
 * other ingestion instances. This ensures redundancy and distributed storage
 * across the datalake.
 */
public class ReplicationManager {

    private static final Logger log = LoggerFactory.getLogger(ReplicationManager.class);

    private final List<String> peers;
    private final int replicationFactor;

    public ReplicationManager(List<String> peers, int replicationFactor) {
        this.peers = peers;
        this.replicationFactor = replicationFactor;
    }

    /**
     * Sends the document content to R-1 peers via HTTP POST.
     *
     * POST /internal/replica/{documentId}
     *
     * This endpoint stores the replica but does not trigger any new broker
     * events.
     */
    public void replicate(String documentId, String content) {

        // If R == 1 or no peers exist, no replication is needed.
        if (peers.isEmpty() || replicationFactor <= 1) {
            log.info("No replication configured (peers={}, R={})", peers.size(), replicationFactor);
            return;
        }

        int replicasNeeded = Math.min(replicationFactor - 1, peers.size());
        int replicasDone = 0;

        try (CloseableHttpClient client = HttpClients.createDefault()) {

            for (String peer : peers) {
                if (replicasDone >= replicasNeeded) {
                    break;
                }

                String url = peer + "/internal/replica/" + documentId;

                // Build POST request
                HttpPost post = new HttpPost(url);
                post.setHeader("Content-Type", "text/plain");
                post.setEntity(new StringEntity(content));

                log.info("Replicating {} to {}", documentId, url);

                client.execute(post, response -> {
                    int code = response.getCode();
                    if (code >= 200 && code < 300) {
                        log.info("Replica to {} OK (HTTP {})", peer, code);
                    } else {
                        log.warn("Replica to {} FAILED (HTTP {})", peer, code);
                    }
                    return null;
                });

                replicasDone++;
            }

        } catch (Exception e) {
            log.error("Error during replication of {}: {}", documentId, e.getMessage(), e);
        }
    }
}
