package es.ulpgc.bigdata.indexing.util;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public final class TextTokenizer {
    private static final Pattern SPLIT = Pattern.compile("[^\\p{L}\\p{Nd}]+");

    public static List<String> tokens(String text) {
        if (text == null || text.isBlank()) return List.of();
        return Arrays.stream(SPLIT.split(text.toLowerCase()))
                .filter(t -> !t.isBlank())
                .toList();
    }
}
