package es.ulpgc.bigdata.ingestion.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper that returns header/body/sourceUrl for a given documentId.
 * Here we assume documentId is an integer representing a Gutenberg book id.
 */
public class DocumentDownloader {

    private static final Logger log = LoggerFactory.getLogger(DocumentDownloader.class);

    public static class DownloadResult {
        public final String header;
        public final String body;
        public final String sourceUrl;

        public DownloadResult(String header, String body, String sourceUrl) {
            this.header = header;
            this.body = body;
            this.sourceUrl = sourceUrl;
        }
    }

    public DownloadResult download(String documentId) {
        try {
            int id = Integer.parseInt(documentId);
            GutenbergDownloader.Result dl = GutenbergDownloader.downloadBook(id);
            GutenbergDownloader.Split split = GutenbergDownloader.splitHeaderBody(dl.content());
            log.info("Downloaded {} from {}", documentId, dl.url());
            return new DownloadResult(split.header(), split.body(), dl.url());
        } catch (Exception e) {
            throw new RuntimeException("Failed to download document " + documentId + ": " + e.getMessage(), e);
        }
    }
}
