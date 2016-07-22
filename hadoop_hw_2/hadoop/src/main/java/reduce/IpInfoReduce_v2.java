package reduce;

import customtypes.IpInfoWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Lev_Khacheresiantc on 7/13/2016.
 */
public class IpInfoReduce_v2
    extends Reducer<Text, IpInfoWritable, Text, IpInfoWritable> {

    private IpInfoWritable ipInfo = new IpInfoWritable(0, 0);

    @Override
    protected void reduce(Text key, Iterable<IpInfoWritable> values,
        Context context) throws IOException, InterruptedException {

        int totalBytes = 0;
        int count = 0;
        for (IpInfoWritable value : values) {
            totalBytes += value.getTotalBytes();
            count += 1;
        }

        ipInfo.setTotalBytes(totalBytes);
        ipInfo.setAverage((double) totalBytes / count);

        context.write(key, ipInfo);
    }
}
