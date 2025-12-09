package es.ulpgc.bigdata.search.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.core.HazelcastInstance;

/**
 * Creates and holds a singleton Hazelcast client for this process.
 *
 * Search-service uses this client to access the distributed inverted index that
 * is being built by the indexing-service.
 *
 * The Hazelcast address and cluster name are configurable through env vars:
 * HAZELCAST_CLUSTER_NAME HAZELCAST_CLUSTER_ADDRESS (host:port, default
 * 127.0.0.1:5701)
 */
public class HazelcastClientProvider {

    private static final Logger log = LoggerFactory.getLogger(HazelcastClientProvider.class);

    private static volatile HazelcastInstance instance;

    private HazelcastClientProvider() {
        // Utility class
    }

    public static HazelcastInstance getInstance() {
        if (instance == null) {
            synchronized (HazelcastClientProvider.class) {
                if (instance == null) {
                    instance = createClient();
                }
            }
        }
        return instance;
    }

    private static HazelcastInstance createClient() {
        String clusterName = System.getenv().getOrDefault("HAZELCAST_CLUSTER_NAME", "dev");
        String address = System.getenv().getOrDefault("HAZELCAST_CLUSTER_ADDRESS", "hazelcast:5701");

        ClientConfig config = new ClientConfig();
        config.setClusterName(clusterName);

        ClientNetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.addAddress(address);

        log.info("Connecting to Hazelcast cluster '{}' at {}", clusterName, address);

        return HazelcastClient.newHazelcastClient(config);
    }

    public static void shutdown() {
        if (instance != null) {
            try {
                instance.shutdown();
            } catch (Exception e) {
                log.warn("Error shutting down Hazelcast client: {}", e.getMessage(), e);
            }
        }
    }
}
