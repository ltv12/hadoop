import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import comporator.CompositeKeyWritable;

import map.BidCountMapper;
import partition.OperationalSystemPartitioner;
import reduce.BidCountReduce;

/**
 * Created by Lev_Khacheresiantc on 7/13/2016.
 */
public class CounterDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CounterDriver(), args);
        System.exit(exitCode);
    }

    private Configuration init() {
        Configuration configuration = super.getConf();
        return configuration;

    }

    public int run(String[] args) throws Exception {
        if (3 != args.length) {
            System.err.printf(
                "Usage: %s [generic options] <input> <output> <key mapping file>\n",
                getClass().getSimpleName());
            return -1;
        }

        Configuration configuration = init();

        Job job = Job.getInstance(configuration, "Count-Driver");

        job.addCacheFile(new Path(args[2]).toUri());

        job.setInputFormatClass(TextInputFormat.class);

        job.setJarByClass(CounterDriver.class);

        job.setMapperClass(BidCountMapper.class);
        job.setReducerClass(BidCountReduce.class);
        job.setPartitionerClass(OperationalSystemPartitioner.class);

        job.setNumReduceTasks(4);
        job.setGroupingComparatorClass(
            CompositeKeyWritable.FirstOnlyComparator.class);
        job.setSortComparatorClass(
            CompositeKeyWritable.FirstOnlyComparator.class);

        job.setMapOutputKeyClass(CompositeKeyWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}