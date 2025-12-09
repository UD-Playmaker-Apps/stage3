package es.ulpgc.bigdata.indexing.api;

import com.hazelcast.cluster.Member;
import es.ulpgc.bigdata.indexing.index.HazelcastIndexProvider;
import io.javalin.Javalin;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class IndexingStatusController {
    private final Javalin app;
    private final HazelcastIndexProvider indexProvider;

    public IndexingStatusController(Javalin app, HazelcastIndexProvider provider) {
        this.app = app;
        this.indexProvider = provider;
    }

    public void registerRoutes() {
        app.get("/health", ctx -> ctx.result("OK"));
        app.get("/index/status", ctx -> {
            Set<Member> members = indexProvider.hazelcast().getCluster().getMembers();
            var nodes = members.stream()
                    .map(m -> Map.of("uuid", m.getUuid().toString(), "address", m.getAddress().toString()))
                    .collect(Collectors.toList());
            ctx.json(Map.of(
                    "clusterName", indexProvider.hazelcast().getConfig().getClusterName(),
                    "members", nodes,
                    "map", "inverted-index",
                    "size", indexProvider.size()
            ));
        });
    }
}
