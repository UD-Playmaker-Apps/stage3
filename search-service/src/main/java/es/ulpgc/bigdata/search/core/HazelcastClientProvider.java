package es.ulpgc.bigdata.search.core;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HazelcastClientProvider {

    private static final Logger log = LoggerFactory.getLogger(HazelcastClientProvider.class);
    private static volatile HazelcastInstance instance;

    private HazelcastClientProvider() {}

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
        String clusterName = System.getenv().getOrDefault("HAZELCAST_CLUSTER_NAME", "search-cluster");
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
