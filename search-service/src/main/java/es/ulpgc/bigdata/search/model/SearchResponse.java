package es.ulpgc.bigdata.search.model;

import java.util.List;

/**
 * JSON payload returned by /search.
 */
public class SearchResponse {

    private String query;
    private int totalHits;
    private List<SearchHit> hits;

    public SearchResponse(String query, List<SearchHit> hits) {
        this.query = query;
        this.hits = hits;
        this.totalHits = hits.size();
    }

    public String getQuery() {
        return query;
    }

    public int getTotalHits() {
        return totalHits;
    }

    public List<SearchHit> getHits() {
        return hits;
    }
}
