package es.ulpgc.bigdata.search.core;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.multimap.MultiMap;
import es.ulpgc.bigdata.search.model.SearchHit;

import java.util.*;
import java.util.stream.Collectors;

public class SearchEngine {

    private final MultiMap<String, String> invertedIndex;

    public SearchEngine(HazelcastInstance hazelcast) {
        this.invertedIndex = hazelcast.getMultiMap("inverted-index");
    }

    // Busca documentos por query, devuelve top N resultados
    public List<SearchHit> search(String queryText, int limit) {
        if (queryText == null || queryText.isBlank()) return Collections.emptyList();

        List<String> terms = tokenize(queryText);

        Map<String, Double> scoreByDoc = new HashMap<>();
        for (String term : terms) {
            Collection<String> docs = invertedIndex.get(term);
            if (docs == null || docs.isEmpty()) continue;
            for (String docId : docs) {
                scoreByDoc.merge(docId, 1.0, Double::sum);
            }
        }

        if (scoreByDoc.isEmpty()) return Collections.emptyList();

        return scoreByDoc.entrySet().stream()
                .map(e -> new SearchHit(e.getKey(), e.getKey(), null, e.getValue()))
                .sorted(Comparator.comparingDouble(SearchHit::getScore).reversed())
                .limit(limit)
                .collect(Collectors.toList());
    }

    // Devuelve documentos que contienen exactamente el término
    public List<SearchHit> searchTerm(String term) {
        Collection<String> docs = invertedIndex.get(term);
        if (docs == null) return Collections.emptyList();
        return docs.stream()
                .map(id -> new SearchHit(id, id, null, 0))
                .collect(Collectors.toList());
    }

    // Tokeniza una query en palabras simples, eliminando caracteres no alfanuméricos
    private List<String> tokenize(String q) {
        return Arrays.stream(q.toLowerCase(Locale.ROOT).split("\\W+"))
                .filter(s -> !s.isBlank())
                .collect(Collectors.toList());
    }
}