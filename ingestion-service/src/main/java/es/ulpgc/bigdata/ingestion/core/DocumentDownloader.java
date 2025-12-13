package es.ulpgc.bigdata.ingestion.core;

public class DocumentDownloader {

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

    public DownloadResult download(String id) {
        try {
            int bookId = Integer.parseInt(id);
            var dl = GutenbergDownloader.downloadBook(bookId);
            var split = GutenbergDownloader.splitHeaderBody(dl.content());
            return new DownloadResult(split.header(), split.body(), dl.url());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
