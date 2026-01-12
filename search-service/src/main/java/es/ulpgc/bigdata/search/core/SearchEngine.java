package es.ulpgc.bigdata.search.core;

import com.hazelcast.collection.ISet;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.multimap.MultiMap;
import es.ulpgc.bigdata.search.model.SearchHit;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SearchEngine {

    // Igual que en indexing-service (TextTokenizer) para consistencia
    private static final Pattern SPLIT = Pattern.compile("[^\\p{L}\\p{Nd}]+");

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
        if (limit <= 0) limit = 10;

        List<String> terms = tokenize(queryText);
        if (terms.isEmpty()) return Collections.emptyList();

        // N = número total de documentos indexados
        int totalDocs = indexedDocs.size();
        if (totalDocs == 0) return Collections.emptyList();

        // TF de la query (evita doble contar si el usuario repite términos)
        Map<String, Integer> queryTf = new HashMap<>();
        for (String t : terms) queryTf.merge(t, 1, Integer::sum);

        Map<String, Double> scoreByDoc = new HashMap<>();

        for (Map.Entry<String, Integer> qEntry : queryTf.entrySet()) {
            String term = qEntry.getKey();
            int qf = qEntry.getValue();

            Collection<String> postings = invertedIndex.get(term);
            if (postings == null || postings.isEmpty()) continue;

            // df = número de documentos distintos que contienen el término
            Set<String> docsWithTerm = new HashSet<>(postings);
            int df = docsWithTerm.size();
            if (df == 0) continue;

            // IDF suavizado: evita idf=0 cuando N=df (muy común con pocos docs)
            // idf = log((N+1)/(df+1)) + 1
            double idf = Math.log((totalDocs + 1.0) / (df + 1.0)) + 1.0;

            // Peso de la query (opcional, pero estándar): (1 + log(qf))
            double qWeight = 1.0 + Math.log(qf);

            // TF por documento contando ocurrencias reales (requiere MultiMap con LIST para duplicados)
            Map<String, Integer> tfByDoc = new HashMap<>();
            for (String docId : postings) {
                tfByDoc.merge(docId, 1, Integer::sum);
            }

            for (Map.Entry<String, Integer> e : tfByDoc.entrySet()) {
                String docId = e.getKey();
                int tf = e.getValue();

                // TF log-normalizado: (1 + log(tf))
                double tfWeight = 1.0 + Math.log(tf);

                double tfidf = (tfWeight * idf) * qWeight;
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
        return Arrays.stream(SPLIT.split(q.toLowerCase(Locale.ROOT)))
                .filter(s -> !s.isBlank())
                .collect(Collectors.toList());
    }
}
