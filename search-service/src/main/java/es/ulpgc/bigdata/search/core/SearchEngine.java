
package es.ulpgc.bigdata.search.core;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.multimap.MultiMap;
import es.ulpgc.bigdata.search.util.QueryTokenizer;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Motor de búsqueda sobre Hazelcast (cliente):
 * - Accede a MultiMap "inverted-index": term -> docId.
 * - Soporta modo OR/AND y ordena por score (# de términos que coinciden).
 * - Paginación con offset/limit.
 */
public class SearchEngine {

    private final HazelcastInstance client;
    private final MultiMap<String, String> invertedIndex;

    private SearchEngine(HazelcastInstance client) {
        this.client = client;
        this.invertedIndex = client.getMultiMap("inverted-index");
    }

    /** Construye SearchEngine desde variables de entorno. */
    public static SearchEngine fromEnv() {
        String clusterName = System.getenv().getOrDefault("HZ_CLUSTER_NAME", "search-cluster");
        String addressesCsv = System.getenv().getOrDefault("HZ_CLIENT_ADDRESSES", "");

        ClientConfig cfg = new ClientConfig();
        cfg.setClusterName(clusterName);

        if (!addressesCsv.isBlank()) {
            ClientNetworkConfig net = cfg.getNetworkConfig();
            for (String addr : addressesCsv.split(",")) {
                String a = addr.trim();
                if (!a.isBlank()) net.addAddress(a);
            }
        }

        HazelcastInstance client = HazelcastClient.newHazelcastClient(cfg);
        return new SearchEngine(client);
    }

    /** Búsqueda con modo OR/AND + paginación y ranking simple. */
    public Map<String, Object> search(String query, String mode, int offset, int limit) {
        List<String> terms = QueryTokenizer.tokens(query);

        // Si no hay términos, devolver vacío
        if (terms.isEmpty()) {
            return Map.of("query", query, "mode", mode, "count", 0, "docs", List.of());
        }

        // Recuperar docs por término
        List<Set<String>> perTermDocs = terms.stream()
                .map(t -> new HashSet<>(Optional.ofNullable(invertedIndex.get(t)).orElse(List.of())))
                .collect(Collectors.toList());

        // Unión (OR) o intersección (AND)
        Set<String> docs;
        if ("and".equalsIgnoreCase(mode)) {
            docs = new HashSet<>(perTermDocs.get(0));
            perTermDocs.stream().skip(1).forEach(docs::retainAll);
        } else {
            docs = new HashSet<>();
            perTermDocs.forEach(docs::addAll);
        }

        // Ranking: # términos que coinciden por doc
        Map<String, Integer> score = new HashMap<>();
        for (String d : docs) score.put(d, 0);
        for (Set<String> set : perTermDocs) {
            for (String d : set) score.computeIfPresent(d, (k, v) -> v + 1);
        }

        // Lista tipada con id y score (evita Object y casts)
        List<DocScore> rankedDocs = docs.stream()
                .map(d -> new DocScore(d, score.getOrDefault(d, 0)))
                .sorted(Comparator.comparingInt(DocScore::score).reversed())
                .skip(Math.max(offset, 0))
                .limit(Math.max(limit, 0))
                .collect(Collectors.toList());

        // Convertir a la estructura que devuelve la API (Map<String,Object>)
        List<Map<String, Object>> ranked = rankedDocs.stream()
                .map(ds -> {
                    Map<String, Object> m = new HashMap<>();
                    m.put("id", ds.id());
                    m.put("score", ds.score());
                    return m;
                })
                .collect(Collectors.toList());


        return Map.of(
                "query", query,
                "mode", mode,
                "count", docs.size(),
                "docs", ranked
        );
    }

    /** Devuelve los docs que contienen un término concreto. */
    public Collection<String> docsForTerm(String term) {
        Collection<String> c = invertedIndex.get(term.toLowerCase());
        return c != null ? c : List.of();
    }

    /** Estadísticas simples del índice (nº de entradas y cluster name). */
    public Map<String, Object> stats() {
        // Nota: invertedIndex.size() devuelve número de entradas (term->docId), no nº de términos.
        return Map.of(
                "clusterName", client.getConfig().getClusterName(),
                "map", "inverted-index",
                "entries", invertedIndex.size()
        );
    }

    /** Cerrar cliente Hazelcast. */
    public void shutdown() {
        client.shutdown();
    }

    /** Clase interna tipada para ranking (evita usar Map y Object durante sort). */
    private static final class DocScore {
        private final String id;
        private final int score;
        DocScore(String id, int score) { this.id = id; this.score = score; }
        String id() { return id; }
        int score() { return score; }
    }
}
