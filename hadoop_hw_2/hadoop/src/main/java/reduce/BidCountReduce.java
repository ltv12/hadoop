package reduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import comporator.CompositeKeyWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by Lev_Khacheresiantc on 7/13/2016.
 */
public class BidCountReduce
    extends Reducer<CompositeKeyWritable, LongWritable, Text, LongWritable> {

    private Map<Integer, String> cityMapping = new HashMap<Integer, String>();

    private LongWritable bidPrice = new LongWritable();
    private Text cityName = new Text();

    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        BufferedReader bufferedReader = null;
        if (cacheFiles != null) {
            try {

                FileSystem fileSystem =
                    FileSystem.get(context.getConfiguration());
                bufferedReader = new BufferedReader(new InputStreamReader(
                    fileSystem.open(new Path(cacheFiles[0]))));
                while (bufferedReader.ready()) {
                    String[] line = bufferedReader.readLine().split("\\s+");

                    Integer cityCode = Integer.valueOf(line[0]);
                    String cityName = line[1];

                    cityMapping.put(cityCode, cityName);
                }
            } finally {
                IOUtils.closeStream(bufferedReader);
            }
        }

    }

    @Override
    protected void reduce(CompositeKeyWritable compositeKey,
        Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {

        long totalPrice = 0;
        for (LongWritable price : values) {
            totalPrice += price.get();
        }

        bidPrice.set(totalPrice);

        String cityName = cityMapping.get(compositeKey.getCityCode());
        this.cityName.set(cityName != null ? cityName
            : Objects.toString(compositeKey.getCityCode()));

        context.write(this.cityName, bidPrice);
    }
}