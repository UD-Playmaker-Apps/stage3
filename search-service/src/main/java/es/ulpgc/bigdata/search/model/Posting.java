package es.ulpgc.bigdata.search.model;

import java.io.Serializable;

/**
 * Represents the occurrence of a term in a single document. This is stored
 * inside the inverted index: term -> list of postings.
 *
 * It is important that this class is Serializable so that Hazelcast can
 * distribute it across the cluster.
 */
public class Posting implements Serializable {

    private String documentId;
    private int termFrequency;
    private double tfIdfScore;

    // Required by Hazelcast serialization
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
