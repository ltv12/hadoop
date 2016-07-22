import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import customtypes.IpInfoWritable;
import map.IpInfoMapper_v1;
import reduce.IpInfoReduce_v1;

/**
 * Created by Lev_Khacheresiantc on 7/13/2016.
 */
public class LogHandlerDriver_v1 extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new LogHandlerDriver_v1(), args);
        System.exit(exitCode);
    }

    private Configuration init() {
        Configuration configuration = new Configuration();
        configuration.set(
            "mapreduce.input.keyvaluelinerecordreader.key.value.separator",
            ",");

        return configuration;

    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",
                getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Configuration configuration = init();

        Job job = Job.getInstance(configuration, "Log parser");

        job.setInputFormatClass(TextInputFormat.class);

        job.setJarByClass(LogHandlerDriver_v1.class);

        job.setMapperClass(IpInfoMapper_v1.class);
        job.setReducerClass(IpInfoReduce_v1.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
