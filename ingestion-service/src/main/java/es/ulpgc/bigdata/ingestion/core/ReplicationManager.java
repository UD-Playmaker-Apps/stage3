package es.ulpgc.bigdata.ingestion.core;

import java.util.List;
import java.util.Map;

import com.google.gson.Gson;

import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles replication of documents between ingestion nodes.
 *
 * Sends JSON payload:
 * {
 *   "header": "...",
 *   "body": "...",
 *   "sourceUrl": "..."
 * }
 */
public class ReplicationManager {

    private static final Logger log = LoggerFactory.getLogger(ReplicationManager.class);
    private final List<String> peers;
    private final int replicationFactor;
    private final Gson gson = new Gson();

    public ReplicationManager(List<String> peers, int replicationFactor) {
        this.peers = peers;
        this.replicationFactor = replicationFactor;
    }

    public void replicate(String documentId, String header, String body, String sourceUrl) {
        if (peers == null || peers.isEmpty() || replicationFactor <= 1) {
            log.info("No replication configured (peers={}, R={})", peers == null ? 0 : peers.size(), replicationFactor);
            return;
        }

        int replicasNeeded = Math.min(replicationFactor - 1, peers.size());
        int replicasDone = 0;

        Map<String, String> payload = Map.of(
                "header", header == null ? "" : header,
                "body", body == null ? "" : body,
                "sourceUrl", sourceUrl == null ? "" : sourceUrl
        );

        try (CloseableHttpClient client = HttpClients.createDefault()) {

            for (String peer : peers) {
                if (replicasDone >= replicasNeeded) break;
                String url = peer.endsWith("/") ? peer + "internal/replica/" + documentId : peer + "/internal/replica/" + documentId;

                HttpPost post = new HttpPost(url);
                post.setHeader("Content-Type", "application/json");
                StringEntity entity = new StringEntity(gson.toJson(payload));
                post.setEntity(entity);

                String p = url;
                log.info("Replicating {} to {}", documentId, p);

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
