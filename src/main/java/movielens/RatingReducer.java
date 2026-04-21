package movielens;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class RatingReducer extends Reducer<Text, Text, Text, Text> {

    private Text result = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        double sum   = 0.0;
        int    count = 0;

        for (Text val : values) {
            try {
                sum += Double.parseDouble(val.toString());
                count++;
            } catch (NumberFormatException e) {
                // ligne malformée, on ignore
            }
        }

        if (count == 0) return;

        double avg = sum / count;
        result.set(String.format("%.2f\t%d", avg, count));
        context.write(key, result);
    }
}
