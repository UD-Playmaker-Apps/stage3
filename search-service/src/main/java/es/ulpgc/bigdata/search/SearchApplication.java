
package es.ulpgc.bigdata.search;

import es.ulpgc.bigdata.search.api.SearchController;
import es.ulpgc.bigdata.search.core.SearchEngine;
import io.javalin.Javalin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entrada del servicio de búsqueda (cliente Hazelcast).
 * - Conecta como cliente al clúster (HZ_CLUSTER_NAME / HZ_CLIENT_ADDRESSES).
 * - Expone /health, /search, /terms/{term}, /stats.
 */
public class SearchApplication {
    private static final Logger log = LoggerFactory.getLogger(SearchApplication.class);

    public static void main(String[] args) {
        // Config de puerto del servicio
        int port = Integer.parseInt(System.getenv().getOrDefault("SEARCH_PORT", "7003"));

        // Inicializar motor de búsqueda (cliente Hazelcast)
        SearchEngine engine = SearchEngine.fromEnv();

        // API con Javalin
        Javalin app = Javalin.create(cfg -> cfg.showJavalinBanner = false);
        new SearchController(app, engine).registerRoutes();
        app.start(port);

        log.info("Search Service started on port {}", port);

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { engine.shutdown(); } catch (Exception ignore) {}
            app.stop();
        }));
    }
}
