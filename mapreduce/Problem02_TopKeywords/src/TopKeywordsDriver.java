import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopKeywordsDriver {

    public static void main(
            String[] args)
            throws Exception {

        /*
         * Expected arguments:
         *
         * args[0] = HDFS input path
         * args[1] = HDFS output path
         */
        if (args.length != 2) {

            System.err.println(
                    "Usage: TopKeywordsDriver <input> <output>"
            );

            System.exit(2);
        }

        Configuration conf =
                new Configuration();

        Job job =
                Job.getInstance(
                        conf,
                        "Top Keywords"
                );

        /*
         * Main class
         */
        job.setJarByClass(
                TopKeywordsDriver.class
        );

        /*
         * Mapper
         */
        job.setMapperClass(
                TopKeywordsMapper.class
        );

        /*
         * Reducer
         */
        job.setReducerClass(
                TopKeywordsReducer.class
        );

        /*
         * IMPORTANT:
         *
         * Use custom CSV InputFormat.
         */
        job.setInputFormatClass(
                MovieCSVInputFormat.class
        );

        /*
         * Mapper output:
         *
         * keyword -> Integer
         */
        job.setMapOutputKeyClass(
                Text.class
        );

        job.setMapOutputValueClass(
                IntWritable.class
        );

        /*
         * Final Reducer output:
         *
         * keyword -> count
         */
        job.setOutputKeyClass(
                Text.class
        );

        job.setOutputValueClass(
                IntWritable.class
        );

        /*
         * HDFS input
         */
        FileInputFormat.addInputPath(
                job,
                new Path(args[0])
        );

        /*
         * HDFS output
         */
        FileOutputFormat.setOutputPath(
                job,
                new Path(args[1])
        );

        /*
         * Start MapReduce job.
         */
        System.exit(
                job.waitForCompletion(true)
                        ? 0
                        : 1
        );
    }
}