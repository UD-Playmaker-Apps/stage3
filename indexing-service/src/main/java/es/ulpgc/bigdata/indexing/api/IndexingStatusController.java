package es.ulpgc.bigdata.indexing.api;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.gson.Gson;
import com.hazelcast.cluster.Member;

import es.ulpgc.bigdata.indexing.index.HazelcastIndexProvider;
import es.ulpgc.bigdata.indexing.util.TextTokenizer;
import io.javalin.Javalin;

public class IndexingStatusController {

    private final Javalin app;
    private final HazelcastIndexProvider indexProvider;
    private final Gson gson = new Gson();

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
                    "terms", indexProvider.terms().size(),
                    "indexedDocs", indexProvider.indexedDocs().size()
            ));
        });

        app.get("/index/search", ctx -> {
            String term = ctx.queryParam("term");
            if (term == null || term.isBlank()) {
                ctx.status(400).result("Missing 'term'");
                return;
            }
            ctx.json(indexProvider.getDocs(term.toLowerCase()));
        });

        app.get("/index/metadata/{id}", ctx -> {
            String id = ctx.pathParam("id");
            var meta = indexProvider.metadataIndex().get(id);
            if (meta == null) {
                ctx.status(404).result("Not Found");
            } else {
                ctx.json(meta);
            }
        });

        app.post("/index/reindex/{id}", ctx -> {
            String id = ctx.pathParam("id");
            if (id == null || id.isBlank()) {
                ctx.status(400).result("Missing id");
                return;
            }

            try {
                // clean old existing entries
                for (String term : indexProvider.terms()) {
                    indexProvider.getDocs(term).removeIf(v -> v.equals(id));
                }
                indexProvider.indexedDocs().remove(id);
                indexProvider.metadataIndex().remove(id);

                Path docDir = Path.of("/data/datalake/docs/", id);
                if (!Files.exists(docDir) || !Files.isDirectory(docDir)) {
                    ctx.status(404).result("Document not found in datalake");
                    return;
                }

                Path headerFile = docDir.resolve("header.txt");
                Path bodyFile = docDir.resolve("body.txt");
                Path metadataFile = docDir.resolve("metadata.json");

                String header = Files.exists(headerFile)
                        ? Files.readString(headerFile, StandardCharsets.UTF_8) : "";
                String body = Files.exists(bodyFile)
                        ? Files.readString(bodyFile, StandardCharsets.UTF_8) : "";
                String content = (header + "\n" + body).trim();

                for (String term : TextTokenizer.tokens(content)) {
                    indexProvider.invertedIndex().put(term, id);
                }
                indexProvider.indexedDocs().add(id);

                if (Files.exists(metadataFile)) {
                    String raw = Files.readString(metadataFile, StandardCharsets.UTF_8);
                    Map<String, Object> metadata = gson.fromJson(raw, Map.class);
                    indexProvider.metadataIndex().put(id, metadata);
                }

                ctx.status(200).result("Reindexed " + id);

            } catch (Exception e) {
                ctx.status(500).result("Reindex failed: " + e.getMessage());
            }
        });
    }
}
