package movielens;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class RatingMapper extends Mapper<Object, Text, Text, Text> {

    private Text movieId = new Text();
    private Text rating  = new Text();

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.isEmpty()) return;

        // Format: userId::movieId::rating::timestamp
        String[] parts = line.split("::");
        if (parts.length != 4) return;

        movieId.set(parts[1]);
        rating.set(parts[2]);

        context.write(movieId, rating);
    }
}
