package map;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by Lev_Khacheresiantc on 7/13/2016.
 */
public class MaxLengthWordMap
    extends Mapper<LongWritable, Text, IntWritable, Text> {

    private IntWritable size = new IntWritable();
    private Text word = new Text();


    public void map(LongWritable docId, Text text, Context ctx)
        throws IOException, InterruptedException {

        String[] words = text.toString()
                .toLowerCase()
                .replaceAll("[^\\p{IsAlphabetic}&&[^\\s^-]]", "")
                .split("\\s+");

        for (String wordStr : words) {
            word.set(wordStr);
            size.set(word.getLength());
            ctx.write(size, word);
        }
    }

}
