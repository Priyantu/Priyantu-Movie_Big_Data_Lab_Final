import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieKeywordsReducer
        extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(
            Text key,
            Iterable<Text> values,
            Context context)
            throws IOException, InterruptedException {

        StringBuilder allKeywords = new StringBuilder();

        for (Text value : values) {

            if (allKeywords.length() > 0) {
                allKeywords.append(", ");
            }

            allKeywords.append(value.toString());
        }

        context.write(key, new Text(allKeywords.toString()));
    }
}