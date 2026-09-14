import org.apache.hadoop.io.LongWritable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopKeywordsMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final Text keyword =
            new Text();

    private static final IntWritable ONE =
            new IntWritable(1);

    @Override
    protected void map(
            LongWritable key,
            Text value,
            Context context)
            throws IOException, InterruptedException {

        String record =
                value.toString().trim();

        /*
         * Ignore empty records.
         */
        if (record.isEmpty()) {
            return;
        }

        /*
         * Parse complete CSV record.
         */
        List<String> fields =
                parseCsv(record);

        /*
         * Need at least 5 columns.
         *
         * keywords = column 4
         */
        if (fields.size() <= 4) {

            context.getCounter(
                    "CSV",
                    "MALFORMED_RECORD"
            ).increment(1);

            return;
        }

        /*
         * Skip header.
         */
        if ("movie_id".equalsIgnoreCase(
                fields.get(0).trim())) {

            return;
        }

        /*
         * Column 4 = keywords
         */
        String keywordsField =
                fields.get(4).trim();

        /*
         * Empty keywords.
         */
        if (keywordsField.isEmpty()
                || keywordsField.equals("[]")) {

            return;
        }

        /*
         * Remove [ and ]
         */
        if (keywordsField.startsWith("[")
                && keywordsField.endsWith("]")) {

            keywordsField =
                    keywordsField.substring(
                            1,
                            keywordsField.length() - 1
                    );
        }

        /*
         * Parse keyword list.
         *
         * Example:
         *
         * 'rescue', 'friendship', 'mission'
         */
        List<String> keywords =
                parseKeywordList(keywordsField);

        for (String item : keywords) {

            String cleaned =
                    item.trim();

            /*
             * Remove single/double quotes.
             */
            if (cleaned.length() >= 2) {

                char first =
                        cleaned.charAt(0);

                char last =
                        cleaned.charAt(
                                cleaned.length() - 1
                        );

                if ((first == '\''
                        && last == '\'')
                        ||
                    (first == '"'
                        && last == '"')) {

                    cleaned =
                            cleaned.substring(
                                    1,
                                    cleaned.length() - 1
                            );
                }
            }

            /*
             * Normalize keyword.
             */
            cleaned =
                    cleaned.trim().toLowerCase();

            /*
             * Ignore empty keyword.
             */
            if (cleaned.isEmpty()) {
                continue;
            }

            keyword.set(cleaned);

            /*
             * Emit:
             *
             * keyword -> 1
             */
            context.write(
                    keyword,
                    ONE
            );
        }
    }

    /*
     * ------------------------------------------------
     * CSV PARSER
     * ------------------------------------------------
     *
     * Correctly handles:
     *
     * "Toy Story","Some text, with comma","..."
     *
     * and:
     *
     * "This field contains
     * multiple lines"
     */
    private List<String> parseCsv(
            String record) {

        List<String> fields =
                new ArrayList<String>();

        StringBuilder current =
                new StringBuilder();

        boolean insideQuotes = false;

        for (int i = 0;
             i < record.length();
             i++) {

            char c =
                    record.charAt(i);

            if (c == '"') {

                /*
                 * Escaped CSV quote:
                 *
                 * ""
                 */
                if (insideQuotes
                        && i + 1 < record.length()
                        && record.charAt(i + 1) == '"') {

                    current.append('"');

                    i++;

                } else {

                    insideQuotes =
                            !insideQuotes;
                }

            } else if (c == ','
                    && !insideQuotes) {

                /*
                 * End of field.
                 */
                fields.add(
                        current.toString()
                );

                current.setLength(0);

            } else {

                current.append(c);
            }
        }

        /*
         * Last field.
         */
        fields.add(
                current.toString()
        );

        return fields;
    }

    /*
     * ------------------------------------------------
     * KEYWORD LIST PARSER
     * ------------------------------------------------
     *
     * Example:
     *
     * ['rescue', 'friendship', 'mission']
     */
    private List<String> parseKeywordList(
            String text) {

        List<String> keywords =
                new ArrayList<String>();

        StringBuilder current =
                new StringBuilder();

        boolean insideSingleQuote = false;

        boolean insideDoubleQuote = false;

        for (int i = 0;
             i < text.length();
             i++) {

            char c =
                    text.charAt(i);

            /*
             * Single quoted keyword
             */
            if (c == '\''
                    && !insideDoubleQuote) {

                insideSingleQuote =
                        !insideSingleQuote;

                current.append(c);

            }

            /*
             * Double quoted keyword
             */
            else if (c == '"'
                    && !insideSingleQuote) {

                insideDoubleQuote =
                        !insideDoubleQuote;

                current.append(c);
            }

            /*
             * Comma outside quotes means
             * next keyword.
             */
            else if (c == ','
                    && !insideSingleQuote
                    && !insideDoubleQuote) {

                keywords.add(
                        current.toString()
                );

                current.setLength(0);

            }

            else {

                current.append(c);
            }
        }

        /*
         * Add final keyword.
         */
        if (current.length() > 0) {

            keywords.add(
                    current.toString()
            );
        }

        return keywords;
    }
}