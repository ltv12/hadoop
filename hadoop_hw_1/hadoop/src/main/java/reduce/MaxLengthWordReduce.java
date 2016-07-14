package reduce;

import map.MaxLengthMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Lev_Khacheresiantc on 7/13/2016.
 */
public class MaxLengthWordReduce
    extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable size = new IntWritable(0);
    private IntWritable maxSize = new IntWritable(0);
    private Text word = new Text();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
        Context context) throws IOException, InterruptedException {
        Iterator<IntWritable> iterator = values.iterator();
        if (iterator.hasNext()) {
            size.set(iterator.next().get());
            if (maxSize.get() < size.get()) {
                maxSize.set(size.get());
                word.set(key);
            }
        }
    }

    @Override
    protected void cleanup(Context context)
        throws IOException, InterruptedException {
        context.write(word, maxSize);
    }
}
