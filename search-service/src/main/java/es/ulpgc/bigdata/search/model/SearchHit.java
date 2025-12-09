package es.ulpgc.bigdata.search.model;

/**
 * A single document returned by the search endpoint, including its aggregated
 * score for the query.
 */
public class SearchHit {

    private String documentId;
    private String title;
    private String path;
    private double score;

    public SearchHit(String documentId, String title, String path, double score) {
        this.documentId = documentId;
        this.title = title;
        this.path = path;
        this.score = score;
    }

    public String getDocumentId() {
        return documentId;
    }

    public String getTitle() {
        return title;
    }

    public String getPath() {
        return path;
    }

    public double getScore() {
        return score;
    }
}
