
package es.ulpgc.bigdata.search.api;

import es.ulpgc.bigdata.search.core.SearchEngine;
import io.javalin.Javalin;

import java.util.Map;
import java.util.Objects;

/**
 * Endpoints HTTP:
 * - GET /health
 * - GET /search?q=...&mode=and|or&offset=0&limit=50
 * - GET /terms/{term}
 * - GET /stats
 */
public class SearchController {
    private final Javalin app;
    private final SearchEngine engine;

    public SearchController(Javalin app, SearchEngine engine) {
        this.app = app;
        this.engine = engine;
    }

    public void registerRoutes() {
        app.get("/health", ctx -> ctx.result("OK"));

        app.get("/search", ctx -> {
            String q = ctx.queryParam("q");
            String mode = ctx.queryParam("mode"); // "or" | "and"
            int offset = Integer.parseInt(Objects.requireNonNull(ctx.queryParam("offset")));
            int limit  = Integer.parseInt(Objects.requireNonNull(ctx.queryParam("limit")));

            var result = engine.search(q, mode, offset, limit);
            ctx.json(result);
        });

        app.get("/terms/{term}", ctx -> {
            String term = ctx.pathParam("term");
            ctx.json(Map.of(
                    "term", term,
                    "docs", engine.docsForTerm(term)
            ));
        });

        app.get("/stats", ctx -> ctx.json(engine.stats()));
    }
}
