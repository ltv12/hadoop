package map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by Lev_Khacheresiantc on 7/13/2016.
 */
public class MaxLengthMap extends Mapper<Object, Text, Text, IntWritable> {

    private Text word = new Text();
    private IntWritable size = new IntWritable();


    public void map(Object o, Text text, Context ctx) throws IOException, InterruptedException {

        String[] words = text.toString()
                .toLowerCase()
                .replaceAll("[^\\p{IsAlphabetic}&&[^\\s^-]]", "")
                .split("\\s+");

        for (String wordStr : words) {
            word.set(wordStr);
            size.set(word.getLength());
            ctx.write(word, size);
        }
    }

}
