import map.MaxLengthMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import reduce.MaxLengthWordReduce;

/**
 * Created by Lev_Khacheresiantc on 7/13/2016.
 */
public class MaxLengthDriver extends Configured implements Tool {

    private final static Logger LOG = Logger.getLogger(MaxLengthDriver.class);

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",
                getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Max length");

        job.setJarByClass(MaxLengthDriver.class);

        job.setMapperClass(MaxLengthMap.class);
        job.setCombinerClass(MaxLengthWordReduce.class);
        job.setReducerClass(MaxLengthWordReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 1 : 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MaxLengthDriver(), args);
        System.exit(exitCode);
    }
}
