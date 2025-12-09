package es.ulpgc.bigdata.search;

import com.google.gson.Gson;
import com.hazelcast.core.HazelcastInstance;
import es.ulpgc.bigdata.search.core.HazelcastClientProvider;
import es.ulpgc.bigdata.search.core.SearchEngine;
import es.ulpgc.bigdata.search.model.SearchHit;
import es.ulpgc.bigdata.search.model.SearchResponse;
import io.javalin.Javalin;
import io.javalin.http.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Main class for the search-service microservice.
 *
 * Responsibilities: - Start Javalin HTTP server. - Connect to the Hazelcast
 * cluster (via HazelcastClientProvider). - Expose REST endpoints to search the
 * distributed index.
 *
 * Endpoints: GET /health GET /search?q=<query>&limit=<n>
 */
public class SearchApplication {

    private static final Logger log = LoggerFactory.getLogger(SearchApplication.class);

    private static final Gson gson = new Gson();

    public static void main(String[] args) {
        int port = resolvePort();

        HazelcastInstance hazelcast = HazelcastClientProvider.getInstance();
        SearchEngine searchEngine = new SearchEngine(hazelcast);

        Javalin app = Javalin.create(config -> {
            config.http.defaultContentType = "application/json";
        });

        // Health check for Docker and Compose
        app.get("/health", ctx -> ctx.json("{\"status\":\"UP\"}"));

        // Search endpoint
        app.get("/search", ctx -> handleSearch(ctx, searchEngine));

        app.exception(Exception.class, (e, ctx) -> {
            log.error("Unhandled exception", e);
            ctx.status(500).json("{\"error\":\"internal server error\"}");
        });

        app.events(events -> events.serverStopped(HazelcastClientProvider::shutdown));

        app.start(port);
        log.info("search-service started on port {}", port);
    }

    private static int resolvePort() {
        String envPort = System.getenv("SEARCH_SERVICE_PORT");
        if (envPort != null && !envPort.isBlank()) {
            try {
                return Integer.parseInt(envPort);
            } catch (NumberFormatException ignored) {
                // fallback to default
            }
        }
        return 8082; // default; match this in docker-compose
    }

    private static void handleSearch(Context ctx, SearchEngine searchEngine) {
        String query = ctx.queryParam("q");
        if (query == null || query.isBlank()) {
            ctx.status(400).json("{\"error\":\"Missing 'q' query parameter\"}");
            return;
        }

        int limit = ctx.queryParamAsClass("limit", Integer.class)
                .getOrDefault(10);

        List<SearchHit> hits = searchEngine.search(query, limit);
        SearchResponse response = new SearchResponse(query, hits);

        ctx.status(200).result(gson.toJson(response));
    }
}
