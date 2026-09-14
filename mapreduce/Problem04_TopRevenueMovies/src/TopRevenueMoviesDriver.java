import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopRevenueMoviesDriver {

    public static void main(String[] args)
            throws Exception {

        if (args.length != 2) {
            System.err.println(
                "Usage: TopRevenueMoviesDriver <input> <output>"
            );
            System.exit(2);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
                conf,
                "Top 20 Highest Revenue Movies"
        );

        job.setJarByClass(
                TopRevenueMoviesDriver.class
        );

        job.setMapperClass(
                TopRevenueMoviesMapper.class
        );

        job.setReducerClass(
                TopRevenueMoviesReducer.class
        );

        job.setInputFormatClass(
                MovieCSVInputFormat.class
        );

        job.setMapOutputKeyClass(
                Text.class
        );

        job.setMapOutputValueClass(
                Text.class
        );

        job.setOutputKeyClass(
                Text.class
        );

        job.setOutputValueClass(
                org.apache.hadoop.io.DoubleWritable.class
        );

        MovieCSVInputFormat.addInputPath(
                job,
                new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
                job,
                new Path(args[1])
        );

        System.exit(
                job.waitForCompletion(true)
                ? 0 : 1
        );
    }
}