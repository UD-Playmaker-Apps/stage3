package es.ulpgc.bigdata.search.core;

import com.hazelcast.collection.ISet;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.multimap.MultiMap;
import es.ulpgc.bigdata.search.model.SearchHit;

import java.util.*;
import java.util.stream.Collectors;

public class SearchEngine {

    private final MultiMap<String, String> invertedIndex;
    private final ISet<String> indexedDocs;
    private final IMap<String, Map<String, Object>> metadataIndex;

    public SearchEngine(HazelcastInstance hazelcast) {
        this.invertedIndex = hazelcast.getMultiMap("inverted-index");
        this.indexedDocs = hazelcast.getSet("indexed-docs");
        this.metadataIndex = hazelcast.getMap("metadata-index");
    }

    public List<SearchHit> search(String queryText, int limit) {
        if (queryText == null || queryText.isBlank()) return Collections.emptyList();

        List<String> terms = tokenize(queryText);
        if (terms.isEmpty()) return Collections.emptyList();

        int totalDocs = indexedDocs.size();
        if (totalDocs == 0) return Collections.emptyList();

        Map<String, Double> scoreByDoc = new HashMap<>();

        for (String term : terms) {
            Collection<String> postings = invertedIndex.get(term);
            if (postings == null || postings.isEmpty()) continue;

            // df = número de documentos distintos que contienen el término
            Set<String> docsWithTerm = new HashSet<>(postings);
            int df = docsWithTerm.size();
            if (df == 0) continue;

            double idf = Math.log((double) totalDocs / (double) df);

            // tf = veces que aparece el término en cada documento
            Map<String, Integer> tfByDoc = new HashMap<>();
            for (String docId : postings) {
                tfByDoc.merge(docId, 1, Integer::sum);
            }

            for (Map.Entry<String, Integer> e : tfByDoc.entrySet()) {
                String docId = e.getKey();
                int tf = e.getValue();
                double tfidf = tf * idf;
                scoreByDoc.merge(docId, tfidf, Double::sum);
            }
        }

        if (scoreByDoc.isEmpty()) return Collections.emptyList();

        return scoreByDoc.entrySet().stream()
                .sorted(Comparator.comparingDouble(Map.Entry<String, Double>::getValue).reversed())
                .limit(limit)
                .map(e -> buildHit(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
    }

    private SearchHit buildHit(String docId, double score) {
        Map<String, Object> meta = metadataIndex.get(docId);

        String title = docId;
        String url = null;

        if (meta != null) {
            Object t = meta.get("title");
            if (t != null) title = t.toString();
            Object s = meta.get("sourceUrl");
            if (s != null) url = s.toString();
        }

        return new SearchHit(docId, title, url, score);
    }

    public List<SearchHit> searchTerm(String term) {
        return search(term, 100);
    }

    private List<String> tokenize(String q) {
        return Arrays.stream(q.toLowerCase(Locale.ROOT).split("\\W+"))
                .filter(s -> !s.isBlank())
                .collect(Collectors.toList());
    }
}