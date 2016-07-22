import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import customtypes.IpInfoWritable;
import map.IpInfoMapper_v2;
import reduce.IpInfoReduce_v2;

/**
 * Created by Lev_Khacheresiantc on 7/13/2016.
 */
public class LogHandlerDriver_v2 extends Configured implements Tool {

    private final static Logger LOG =
        Logger.getLogger(LogHandlerDriver_v2.class);

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new LogHandlerDriver_v2(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",
                getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Max length");

        job.setInputFormatClass(TextInputFormat.class);

        job.setJarByClass(LogHandlerDriver_v2.class);

        job.setMapperClass(IpInfoMapper_v2.class);
        job.setReducerClass(IpInfoReduce_v2.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IpInfoWritable.class);

        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
