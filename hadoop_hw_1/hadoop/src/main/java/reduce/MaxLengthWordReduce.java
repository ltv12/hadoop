package reduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by Lev_Khacheresiantc on 7/13/2016.
 */
public class MaxLengthWordReduce
    extends Reducer<IntWritable, Text, IntWritable, Text> {

    private Text words = new Text();

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        setup(context);
        try {
            if (context.nextKey()) {
                StringBuilder sb = new StringBuilder();
                for (Text word : context.getValues()) {
                    sb.append(word.toString()).append(" ");
                }
                words.set(sb.toString());
                context.write(context.getCurrentKey(), words);
            }
        } finally {
            cleanup(context);
        }
    }
}
