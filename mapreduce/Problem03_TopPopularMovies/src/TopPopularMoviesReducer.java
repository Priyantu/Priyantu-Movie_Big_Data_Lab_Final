import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopPopularMoviesReducer
        extends Reducer<Text, Text, Text, DoubleWritable> {

    private final List<MoviePopularity> movies =
            new ArrayList<MoviePopularity>();

    @Override
    protected void reduce(
            Text key,
            Iterable<Text> values,
            Context context)
            throws IOException, InterruptedException {

        for (Text value : values) {

            String data = value.toString();

            int separator = data.lastIndexOf('\t');

            if (separator == -1) {
                continue;
            }

            String title =
                    data.substring(0, separator).trim();

            String popularityText =
                    data.substring(separator + 1).trim();

            try {

                double popularity =
                        Double.parseDouble(popularityText);

                movies.add(
                        new MoviePopularity(
                                title,
                                popularity
                        )
                );

            } catch (NumberFormatException e) {

                context.getCounter(
                        "CSV",
                        "INVALID_POPULARITY_REDUCER"
                ).increment(1);
            }
        }
    }

    @Override
    protected void cleanup(
            Context context)
            throws IOException, InterruptedException {

        // Highest popularity first
        Collections.sort(
                movies,
                new Comparator<MoviePopularity>() {

                    @Override
                    public int compare(
                            MoviePopularity a,
                            MoviePopularity b) {

                        return Double.compare(
                                b.popularity,
                                a.popularity
                        );
                    }
                }
        );

        int topN =
                Math.min(20, movies.size());

        for (int i = 0; i < topN; i++) {

            MoviePopularity movie =
                    movies.get(i);

            context.write(
                    new Text(movie.title),
                    new DoubleWritable(
                            movie.popularity
                    )
            );
        }
    }

    private static class MoviePopularity {

        String title;
        double popularity;

        MoviePopularity(
                String title,
                double popularity) {

            this.title = title;
            this.popularity = popularity;
        }
    }
}