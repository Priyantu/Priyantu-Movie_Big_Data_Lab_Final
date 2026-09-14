import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopPopularMoviesMapper
        extends Mapper<LongWritable, Text, Text, Text> {

    private final Text movieId = new Text();
    private final Text movieData = new Text();

    @Override
    protected void map(
            LongWritable key,
            Text value,
            Context context)
            throws IOException, InterruptedException {

        String record = value.toString().trim();

        if (record.isEmpty()) {
            return;
        }

        List<String> fields = parseCsv(record);

        // Expected columns:
        // 0 = movie_id
        // 1 = title
        // 8 = popularity

        if (fields.size() <= 8) {
            context.getCounter(
                    "CSV",
                    "MALFORMED_RECORD"
            ).increment(1);
            return;
        }

        // Skip header
        if ("movie_id".equalsIgnoreCase(
                fields.get(0).trim())) {
            return;
        }

        String id = fields.get(0).trim();
        String title = fields.get(1).trim();
        String popularityText = fields.get(8).trim();

        if (id.isEmpty() ||
                title.isEmpty() ||
                popularityText.isEmpty()) {
            return;
        }

        try {
            double popularity =
                    Double.parseDouble(popularityText);

            movieId.set(id);

            // title + tab + popularity
            movieData.set(
                    title + "\t" + popularity
            );

            context.write(movieId, movieData);

        } catch (NumberFormatException e) {

            context.getCounter(
                    "CSV",
                    "INVALID_POPULARITY"
            ).increment(1);
        }
    }

    private List<String> parseCsv(String record) {

        List<String> fields =
                new ArrayList<String>();

        StringBuilder current =
                new StringBuilder();

        boolean insideQuotes = false;

        for (int i = 0; i < record.length(); i++) {

            char c = record.charAt(i);

            if (c == '"') {

                if (insideQuotes &&
                        i + 1 < record.length() &&
                        record.charAt(i + 1) == '"') {

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

        fields.add(current.toString());

        return fields;
    }
}