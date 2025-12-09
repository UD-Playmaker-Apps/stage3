package es.ulpgc.bigdata.search.core;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import es.ulpgc.bigdata.search.model.DocumentMetadata;
import es.ulpgc.bigdata.search.model.Posting;
import es.ulpgc.bigdata.search.model.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Core search logic: - Takes a textual query - Tokenizes it into terms - For
 * each term, reads the posting list from the inverted index map - Aggregates
 * scores per document - Looks up document metadata and returns the best hits.
 *
 * This class is intentionally stateless; it always reads from Hazelcast.
 */
public class SearchEngine {

    private static final Logger log = LoggerFactory.getLogger(SearchEngine.class);

    private final IMap<String, List<Posting>> invertedIndex;
    private final IMap<String, DocumentMetadata> documentsMetadata;

    public SearchEngine(HazelcastInstance hazelcast) {
        // These map names must match what indexing-service uses
        this.invertedIndex = hazelcast.getMap("inverted-index");
        this.documentsMetadata = hazelcast.getMap("documents");
    }

    /**
     * Executes a search for the given query string.
     *
     * @param queryText user query, e.g. "big data systems"
     * @param limit max number of hits to return
     */
    public List<SearchHit> search(String queryText, int limit) {
        if (queryText == null || queryText.isBlank()) {
            return Collections.emptyList();
        }

        // Simple tokenization: lower-case, split on non-word characters.
        List<String> terms = tokenize(queryText);

        Map<String, Double> scoreByDocument = new HashMap<>();

        for (String term : terms) {
            List<Posting> postings = invertedIndex.get(term);
            if (postings == null || postings.isEmpty()) {
                continue;
            }

            for (Posting posting : postings) {
                // We assume tfIdfScore already includes IDF information.
                scoreByDocument.merge(posting.getDocumentId(),
                        posting.getTfIdfScore(),
                        Double::sum);
            }
        }

        if (scoreByDocument.isEmpty()) {
            return Collections.emptyList();
        }

        // Convert to SearchHit objects and sort by score descending.
        return scoreByDocument.entrySet().stream()
                .map(entry -> {
                    String docId = entry.getKey();
                    double score = entry.getValue();
                    DocumentMetadata metadata = documentsMetadata.get(docId);

                    String title = metadata != null ? metadata.getTitle() : docId;
                    String path = metadata != null ? metadata.getPath() : null;

                    return new SearchHit(docId, title, path, score);
                })
                .sorted(Comparator.comparingDouble(SearchHit::getScore).reversed())
                .limit(limit)
                .collect(Collectors.toList());
    }

    private List<String> tokenize(String queryText) {
        return Arrays.stream(queryText
                .toLowerCase(Locale.ROOT)
                .split("\\W+"))
                .filter(s -> !s.isBlank())
                .collect(Collectors.toList());
    }
}
