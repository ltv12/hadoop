package partition;

import comporator.CompositeKeyWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by Lev_Khacheresiantc on 7/22/2016.
 */
public class OperationalSystemPartitioner
    extends Partitioner<CompositeKeyWritable, LongWritable> {
    @Override
    public int getPartition(CompositeKeyWritable compositeKey,
        LongWritable longWritable, int numReduceTasks) {
        if (numReduceTasks == 0) {
            return 0;
        } else {
            return (compositeKey.getOperationalSystem().hashCode()
                & Integer.MAX_VALUE) % numReduceTasks;
        }
    }
}
