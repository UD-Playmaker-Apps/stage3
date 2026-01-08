package es.ulpgc.bigdata.search.model;

import java.io.Serializable;

public class Posting implements Serializable {

    private String documentId;
    private int termFrequency;
    private double tfIdfScore;

    public Posting() {
    }

    public Posting(String documentId, int termFrequency, double tfIdfScore) {
        this.documentId = documentId;
        this.termFrequency = termFrequency;
        this.tfIdfScore = tfIdfScore;
    }

    public String getDocumentId() {
        return documentId;
    }

    public int getTermFrequency() {
        return termFrequency;
    }

    public double getTfIdfScore() {
        return tfIdfScore;
    }

    public void setDocumentId(String documentId) {
        this.documentId = documentId;
    }

    public void setTermFrequency(int termFrequency) {
        this.termFrequency = termFrequency;
    }

    public void setTfIdfScore(double tfIdfScore) {
        this.tfIdfScore = tfIdfScore;
    }
}
