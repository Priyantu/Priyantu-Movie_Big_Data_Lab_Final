import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopPopularMoviesDriver {

    public static void main(String[] args)
            throws Exception {

        if (args.length != 2) {
            System.err.println(
                    "Usage: TopPopularMoviesDriver <input> <output>"
            );
            System.exit(2);
        }

        Configuration conf =
                new Configuration();

        Job job =
                Job.getInstance(
                        conf,
                        "Top 20 Most Popular Movies"
                );

        job.setJarByClass(
                TopPopularMoviesDriver.class
        );

        job.setMapperClass(
                TopPopularMoviesMapper.class
        );

        job.setReducerClass(
                TopPopularMoviesReducer.class
        );

        // Custom CSV reader for multiline CSV records
        job.setInputFormatClass(
                MovieCSVInputFormat.class
        );

        // Mapper output types
        job.setMapOutputKeyClass(
                Text.class
        );

        job.setMapOutputValueClass(
                Text.class
        );

        // Reducer / final output types
        job.setOutputKeyClass(
                Text.class
        );

        job.setOutputValueClass(
                DoubleWritable.class
        );

        FileInputFormat.addInputPath(
                job,
                new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
                job,
                new Path(args[1])
        );

        System.exit(
                job.waitForCompletion(true)
                        ? 0
                        : 1
        );
    }
}