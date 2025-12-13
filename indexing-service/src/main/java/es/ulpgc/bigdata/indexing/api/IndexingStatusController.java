package es.ulpgc.bigdata.indexing.api;

import com.hazelcast.cluster.Member;
import es.ulpgc.bigdata.indexing.index.HazelcastIndexProvider;
import es.ulpgc.bigdata.indexing.util.TextTokenizer;
import io.javalin.Javalin;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
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

        app.get("/index/search", ctx -> {
            String term = ctx.queryParam("term");
            if (term == null || term.isBlank()) {
                ctx.status(400).result("Missing 'term' query parameter");
                return;
            }
            Collection<String> docs = indexProvider.getDocs(term.toLowerCase());
            ctx.json(docs);
        });

        app.get("/index/terms/{id}", ctx -> {
            String id = ctx.pathParam("id");
            if (id == null || id.isBlank()) {
                ctx.status(400).result("Missing id");
                return;
            }
            List<String> terms = indexProvider.terms().stream()
                    .filter(t -> indexProvider.getDocs(t).contains(id))
                    .collect(Collectors.toList());
            ctx.json(terms);
        });

        app.post("/index/reindex/{id}", ctx -> {
            String id = ctx.pathParam("id");
            if (id == null || id.isBlank()) {
                ctx.status(400).result("Missing id");
                return;
            }

            try {
                // Eliminar el documento de términos anteriores
                for (String term : indexProvider.terms()) {
                    indexProvider.getDocs(term).remove(id);
                }
                indexProvider.indexedDocs().remove(id);

                // Leer contenido directamente desde datalake
                String content = null;
                Path docDir = Path.of("/data/datalake/docs/", id);
                if (Files.exists(docDir) && Files.isDirectory(docDir)) {
                    String header = "";
                    String body = "";
                    Path headerFile = docDir.resolve("header.txt");
                    Path bodyFile = docDir.resolve("body.txt");
                    if (Files.exists(headerFile)) header = Files.readString(headerFile, StandardCharsets.UTF_8);
                    if (Files.exists(bodyFile)) body = Files.readString(bodyFile, StandardCharsets.UTF_8);
                    content = (header + "\n" + body).trim();
                }

                if (content == null || content.isBlank()) {
                    ctx.status(404).result("Document not found in datalake");
                    return;
                }

                // Reindexar contenido
                for (String term : TextTokenizer.tokens(content)) {
                    indexProvider.invertedIndex().put(term, id);
                }
                indexProvider.indexedDocs().add(id);

                ctx.status(200).result("Reindexed " + id);
            } catch (Exception e) {
                ctx.status(500).result("Reindex failed: " + e.getMessage());
            }
        });
    }
}
