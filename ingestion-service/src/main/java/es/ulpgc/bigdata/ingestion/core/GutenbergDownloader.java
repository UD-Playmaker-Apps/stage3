package es.ulpgc.bigdata.ingestion.core;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

/**
 * Reused/adapted from Stage-2. Downloads Gutenberg books and splits header/body.
 */
public final class GutenbergDownloader {

    public static record Result(String url, String content) { }

    private static final List<String> patterns = List.of(
            "https://www.gutenberg.org/files/%d/%d-0.txt",
            "https://www.gutenberg.org/files/%d/%d.txt",
            "https://www.gutenberg.org/ebooks/%d.txt.utf-8"
    );

    private static final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    public static Result downloadBook(int bookId) throws Exception {
        Exception last = null;
        for (String p : patterns) {
            String url = String.format(p, bookId, bookId);
            try {
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .timeout(Duration.ofSeconds(30))
                        .GET()
                        .build();
                HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
                if (res.statusCode() == 200 && res.body() != null && !res.body().isBlank()) {
                    return new Result(url, res.body());
                }
            } catch (Exception e) {
                last = e;
            }
        }
        throw new RuntimeException("Unable to fetch Gutenberg text for id=" + bookId, last);
    }

    public static record Split(String header, String body) { }

    public static Split splitHeaderBody(String text) {
        if (text == null || text.isBlank()) return new Split("", "");

        String start = "*** START OF THE PROJECT GUTENBERG EBOOK";
        String end = "*** END OF THE PROJECT GUTENBERG EBOOK";

        String upper = text.toUpperCase();
        int s = upper.indexOf(start);
        int e = upper.indexOf(end);

        if (s >= 0) {
            int bodyStart = text.indexOf('\n', s);
            if (bodyStart < 0) bodyStart = s;
            String header = text.substring(0, s).trim();
            String body;
            if (e > s) {
                body = text.substring(bodyStart, e).trim();
            } else {
                body = text.substring(bodyStart).trim();
            }
            return new Split(header, body);
        }

        // fallback: split after N lines
        String[] lines = text.split("\\R", -1);
        int boundary = Math.min(200, lines.length);
        StringBuilder h = new StringBuilder();
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < lines.length; i++) {
            if (i < boundary) h.append(lines[i]).append('\n');
            else b.append(lines[i]).append('\n');
        }
        return new Split(h.toString().trim(), b.toString().trim());
    }
}
