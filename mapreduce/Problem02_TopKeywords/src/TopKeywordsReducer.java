import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopKeywordsReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final List<KeywordCount> keywordCounts =
            new ArrayList<KeywordCount>();

    @Override
    protected void reduce(
            Text key,
            Iterable<IntWritable> values,
            Context context)
            throws IOException, InterruptedException {

        int sum = 0;

        for (IntWritable value : values) {
            sum += value.get();
        }

        keywordCounts.add(
                new KeywordCount(
                        key.toString(),
                        sum
                )
        );
    }

    @Override
    protected void cleanup(
            Context context)
            throws IOException, InterruptedException {

        Collections.sort(
                keywordCounts,
                new Comparator<KeywordCount>() {

                    @Override
                    public int compare(
                            KeywordCount a,
                            KeywordCount b) {

                        int countCompare =
                                Integer.compare(
                                        b.count,
                                        a.count
                                );

                        if (countCompare != 0) {
                            return countCompare;
                        }

                        return a.keyword.compareTo(
                                b.keyword
                        );
                    }
                }
        );

        int topN = 20;

        int limit =
                Math.min(
                        topN,
                        keywordCounts.size()
                );

        for (int i = 0; i < limit; i++) {

            KeywordCount item =
                    keywordCounts.get(i);

            context.write(
                    new Text(item.keyword),
                    new IntWritable(item.count)
            );
        }
    }

    private static class KeywordCount {

        String keyword;
        int count;

        KeywordCount(
                String keyword,
                int count) {

            this.keyword = keyword;
            this.count = count;
        }
    }
}