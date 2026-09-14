import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopRevenueMoviesReducer
        extends Reducer<Text, Text, Text, DoubleWritable> {

    private final List<MovieRevenue> movies =
            new ArrayList<MovieRevenue>();

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

            String revenueText =
                    data.substring(separator + 1).trim();

            try {

                double revenue =
                        Double.parseDouble(revenueText);

                movies.add(
                        new MovieRevenue(
                                title,
                                revenue
                        )
                );

            } catch (NumberFormatException e) {

                context.getCounter(
                        "CSV",
                        "INVALID_REVENUE_REDUCER"
                ).increment(1);
            }
        }
    }

    @Override
    protected void cleanup(
            Context context)
            throws IOException, InterruptedException {

        // Highest revenue first
        Collections.sort(
                movies,
                new Comparator<MovieRevenue>() {

                    @Override
                    public int compare(
                            MovieRevenue a,
                            MovieRevenue b) {

                        return Double.compare(
                                b.revenue,
                                a.revenue
                        );
                    }
                }
        );

        int topN =
                Math.min(20, movies.size());

        for (int i = 0; i < topN; i++) {

            MovieRevenue movie =
                    movies.get(i);

            context.write(
                    new Text(movie.title),
                    new DoubleWritable(
                            movie.revenue
                    )
            );
        }
    }

    private static class MovieRevenue {

        String title;
        double revenue;

        MovieRevenue(
                String title,
                double revenue) {

            this.title = title;
            this.revenue = revenue;
        }
    }
}