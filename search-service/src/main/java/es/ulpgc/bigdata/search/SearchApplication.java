package es.ulpgc.bigdata.search;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.HazelcastInstance;

import es.ulpgc.bigdata.search.core.HazelcastClientProvider;
import es.ulpgc.bigdata.search.core.SearchEngine;
import es.ulpgc.bigdata.search.model.SearchResponse;
import io.javalin.Javalin;
import io.javalin.http.Context;

public class SearchApplication {

    private static final Logger log = LoggerFactory.getLogger(SearchApplication.class);

    public static void main(String[] args) {

        int port = resolvePort();

        HazelcastInstance hazelcast = HazelcastClientProvider.getInstance();
        SearchEngine searchEngine = new SearchEngine(hazelcast);

        Javalin app = Javalin.create(config -> {
            config.http.defaultContentType = "application/json";
            config.showJavalinBanner = false;
        });

        app.get("/health", ctx -> ctx.json(Map.of("status", "UP")));

        app.get("/search", ctx -> handleSearch(ctx, searchEngine));

        // Direct search for a specific term
        app.get("/index/terms/{term}", ctx -> {
            String term = ctx.pathParam("term");
            var hits = searchEngine.search(term, 100);
            ctx.json(hits);
        });

        app.exception(Exception.class, (e, ctx) -> {
            log.error("Unhandled exception", e);
            ctx.status(500).json(Map.of("error", "internal server error"));
        });

        app.events(events -> events.serverStopped(HazelcastClientProvider::shutdown));

        app.start(port);
        log.info("Search-service started on port {}", port);
    }

    private static int resolvePort() {
        String envPort = System.getenv("SEARCH_PORT");
        if (envPort != null && !envPort.isBlank()) {
            try {
                return Integer.parseInt(envPort);
            } catch (NumberFormatException ignored) {
            }
        }
        return 7004; // default consistent with our docker-compose
    }

    private static void handleSearch(Context ctx, SearchEngine searchEngine) {
        String query = ctx.queryParam("q");
        if (query == null || query.isBlank()) {
            ctx.status(400).json(Map.of("error", "Missing 'q' query parameter"));
            return;
        }

        int limit = ctx.queryParamAsClass("limit", Integer.class).getOrDefault(10);

        List<?> hits = searchEngine.search(query, limit);

        ctx.status(200).json(new SearchResponse(query, (List) hits));
    }
}
