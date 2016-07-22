package reduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by Lev_Khacheresiantc on 7/13/2016.
 */
public class IpInfoReduce_v1 extends Reducer<Text, Text, Text, Text> {

    private Text value = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

        int count = 0;
        int totalBytes = 0;
        for (Text countAndBytes : values) {
            String[] data = countAndBytes.toString().split("\\t");
            count += Integer.parseInt(data[0]);
            totalBytes += Integer.parseInt(data[1]);
        }

        value.set(
            String.format("%.2f,%d", totalBytes / (double) count, totalBytes));

        context.write(key, value);
    }
}
