import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class MovieCSVInputFormat
        extends FileInputFormat<LongWritable, Text> {

    /*
     * CSV file contains multiline quoted fields.
     *
     * Therefore Hadoop must NOT split this file
     * into multiple physical input splits.
     */
    @Override
    protected boolean isSplitable(
            JobContext context,
            org.apache.hadoop.fs.Path filename) {

        return false;
    }

    @Override
    public RecordReader<LongWritable, Text>
            createRecordReader(
                    InputSplit split,
                    TaskAttemptContext context)
            throws IOException {

        return new CSVRecordReader();
    }
}