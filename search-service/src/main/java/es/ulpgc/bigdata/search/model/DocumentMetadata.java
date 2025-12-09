package es.ulpgc.bigdata.search.model;

import java.io.Serializable;

/**
 * Basic metadata about a document. This allows the search-service to present
 * user-friendly results instead of only returning the documentId.
 */
public class DocumentMetadata implements Serializable {

    private String id;
    private String title;
    private String path;

    // Required by Hazelcast serialization
    public DocumentMetadata() {
    }

    public DocumentMetadata(String id, String title, String path) {
        this.id = id;
        this.title = title;
        this.path = path;
    }

    public String getId() {
        return id;
    }

    public String getTitle() {
        return title;
    }

    public String getPath() {
        return path;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setPath(String path) {
        this.path = path;
    }
}
