package es.ulpgc.bigdata.search.model;

public class SearchHit {

    private String documentId;
    private String title;
    private String url;
    private double score;

    public SearchHit(String documentId, String title, String url, double score) {
        this.documentId = documentId;
        this.title = title;
        this.url = url;
        this.score = score;
    }

    public String getDocumentId() { return documentId; }
    public String getTitle() { return title; }
    public String getUrl() { return url; }
    public double getScore() { return score; }
}
