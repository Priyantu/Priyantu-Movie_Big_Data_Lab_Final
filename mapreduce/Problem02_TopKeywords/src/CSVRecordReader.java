import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CSVRecordReader
        extends RecordReader<LongWritable, Text> {

    private BufferedReader reader;

    private final LongWritable currentKey =
            new LongWritable();

    private final Text currentValue =
            new Text();

    private long recordNumber = 0;

    private boolean finished = false;

    @Override
    public void initialize(
            InputSplit genericSplit,
            TaskAttemptContext context)
            throws IOException {

        FileSplit split = (FileSplit) genericSplit;

        Configuration conf =
                context.getConfiguration();

        Path file = split.getPath();

        FileSystem fs =
                file.getFileSystem(conf);

        FSDataInputStream inputStream =
                fs.open(file);

        reader = new BufferedReader(
                new InputStreamReader(
                        inputStream,
                        StandardCharsets.UTF_8
                )
        );
    }

    @Override
    public boolean nextKeyValue()
            throws IOException {

        if (finished) {
            return false;
        }

        StringBuilder record =
                new StringBuilder();

        boolean insideQuotes = false;

        while (true) {

            int ch = reader.read();

            /*
             * End of file
             */
            if (ch == -1) {

                if (record.length() == 0) {
                    finished = true;
                    return false;
                }

                currentKey.set(recordNumber++);

                currentValue.set(
                        record.toString().trim()
                );

                finished = true;

                return true;
            }

            char c = (char) ch;

            /*
             * Handle double quotes.
             *
             * CSV escaped quote:
             *
             * ""
             */
            if (c == '"') {

                if (insideQuotes) {

                    reader.mark(1);

                    int next = reader.read();

                    if (next == '"') {

                        /*
                         * Escaped quote
                         */
                        record.append('"');
                        record.append('"');

                    } else {

                        /*
                         * Closing quote
                         */
                        insideQuotes = false;

                        record.append(c);

                        if (next != -1) {
                            reader.reset();
                        }
                    }

                } else {

                    /*
                     * Opening quote
                     */
                    insideQuotes = true;

                    record.append(c);
                }

            } else {

                record.append(c);
            }

            /*
             * Newline outside quotes means
             * the CSV record is complete.
             *
             * Newline inside quotes belongs to
             * the current field.
             */
            if (c == '\n' && !insideQuotes) {

                currentKey.set(recordNumber++);

                currentValue.set(
                        record.toString().trim()
                );

                return true;
            }
        }
    }

    @Override
    public LongWritable getCurrentKey() {
        return currentKey;
    }

    @Override
    public Text getCurrentValue() {
        return currentValue;
    }

    @Override
    public float getProgress()
            throws IOException {

        if (finished) {
            return 1.0f;
        }

        return 0.0f;
    }

    @Override
    public void close()
            throws IOException {

        if (reader != null) {
            reader.close();
        }
    }
}