package es.ulpgc.bigdata.ingestion.core;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

public class MetadataFetcher {

    public static class Metadata {
        public final String title;
        public final String author;
        public final String language;
        public final String releaseDate;

        public Metadata(String title, String author, String language, String releaseDate) {
            this.title = title;
            this.author = author;
            this.language = language;
            this.releaseDate = releaseDate;
        }
    }

    public Metadata fetch(int id) {
        try {
            String url = "https://www.gutenberg.org/ebooks/" + id;
            Document doc = Jsoup.connect(url).get();

            String title = "Unknown";
            String author = "Unknown";

            Element h1 = doc.selectFirst("h1");
            if (h1 != null) {
                String[] parts = h1.text().split(" by ", 2);
                title = clean(parts[0]);
                if (parts.length > 1) author = clean(parts[1]);
            }

            String language = "Unknown";
            String releaseDate = "";

            Element table = doc.selectFirst("table.bibrec");
            if (table != null) {
                for (Element row : table.select("tr")) {
                    Element th = row.selectFirst("th");
                    Element td = row.selectFirst("td");
                    if (th == null || td == null) continue;

                    String key = th.text().trim();
                    String value = td.text().trim();

                    if (key.equalsIgnoreCase("Language")) {
                        language = value;
                    }
                    if (key.equalsIgnoreCase("Release Date")) {
                        releaseDate = value;
                    }
                }
            }

            return new Metadata(title, author, language, releaseDate);

        } catch (Exception e) {
            return new Metadata("Unknown", "Unknown", "Unknown", "");
        }
    }

    private String clean(String s) {
        return s.replaceAll("[/:*?\"<>|]", "").trim();
    }
}
