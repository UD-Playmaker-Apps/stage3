package es.ulpgc.bigdata.ingestion.core;

import com.google.gson.Gson;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.StringEntity;

import java.util.List;
import java.util.Map;

public class ReplicationManager {

    private final List<String> peers;
    private final int replicationFactor;
    private final Gson gson = new Gson();

    public ReplicationManager(List<String> peers, int replicationFactor) {
        this.peers = peers;
        this.replicationFactor = replicationFactor;
    }

    public void replicate(String documentId, String header, String body, String sourceUrl,
                          Map<String, Object> metadata) {

        if (peers == null || peers.isEmpty() || replicationFactor <= 1) return;

        int replicasNeeded = Math.min(replicationFactor - 1, peers.size());
        int replicasDone = 0;

        Map<String, Object> payload = Map.of(
                "header", header == null ? "" : header,
                "body", body == null ? "" : body,
                "sourceUrl", sourceUrl == null ? "" : sourceUrl,
                "metadata", metadata == null ? Map.of() : metadata
        );

        try (CloseableHttpClient client = HttpClients.createDefault()) {
            for (String peer : peers) {
                if (replicasDone >= replicasNeeded) break;

                String url = peer.endsWith("/")
                        ? peer + "internal/replica/" + documentId
                        : peer + "/internal/replica/" + documentId;

                HttpPost post = new HttpPost(url);
                post.setHeader("Content-Type", "application/json");
                post.setEntity(new StringEntity(gson.toJson(payload)));

                client.execute(post, response -> null);
                replicasDone++;
            }
        } catch (Exception ignored) {}
    }
}
