import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieKeywordsMapper
        extends Mapper<LongWritable, Text, Text, Text> {

    private final Text movieTitle = new Text();
    private final Text keywords = new Text();

    @Override
    protected void map(
            LongWritable key,
            Text value,
            Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // Skip header
        if (line.startsWith("movie_id,title,overview")) {
            return;
        }

        List<String> fields = parseCSV(line);

        // We need at least:
        // 0 = movie_id
        // 1 = title
        // 2 = overview
        // 3 = genres
        // 4 = keywords
        if (fields.size() < 5) {
            return;
        }

        String title = fields.get(1).trim();
        String keywordList = fields.get(4).trim();

        // Ignore movies without keywords
        if (title.isEmpty() ||
                keywordList.isEmpty() ||
                keywordList.equals("[]")) {
            return;
        }

        movieTitle.set(title);
        keywords.set(keywordList);

        context.write(movieTitle, keywords);
    }

    /*
     * CSV parser that handles:
     * - fields surrounded by "
     * - commas inside quoted fields
     * - escaped quotes ("")
     */
    private List<String> parseCSV(String line) {

        List<String> fields = new ArrayList<String>();

        StringBuilder current = new StringBuilder();

        boolean insideQuotes = false;

        for (int i = 0; i < line.length(); i++) {

            char c = line.charAt(i);

            if (c == '"') {

                // Escaped double quote: ""
                if (insideQuotes &&
                        i + 1 < line.length() &&
                        line.charAt(i + 1) == '"') {

                    current.append('"');
                    i++;

                } else {
                    insideQuotes = !insideQuotes;
                }

            } else if (c == ',' && !insideQuotes) {

                fields.add(current.toString());
                current.setLength(0);

            } else {

                current.append(c);
            }
        }

        // Add last field
        fields.add(current.toString());

        return fields;
    }
}
