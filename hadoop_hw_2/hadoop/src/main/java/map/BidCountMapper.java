package map;

import java.io.IOException;
import java.util.regex.Matcher;

import comporator.CompositeKeyWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import static utlils.LogParserUtils.*;

/**
 * Created by Lev_Khacheresiantc on 7/13/2016.
 */
public class BidCountMapper
    extends Mapper<LongWritable, Text, CompositeKeyWritable, LongWritable> {

    private static final int UNINTERESTING_PRICE = 250;

    private CompositeKeyWritable compositeKey = new CompositeKeyWritable();
    private LongWritable bidPrice = new LongWritable(0);

    public void map(LongWritable docId, Text text, Context ctx)
        throws IOException, InterruptedException {

        Matcher matcher = LOG_PATTERN.matcher(text.toString());

        if (matcher.matches()) {
            long price = Long.parseLong(matcher.group(Groups.BID_PRICE));
            Integer cityCode = Integer.valueOf(matcher.group(Groups.CITY));
            String os = operationalSystem(matcher.group(Groups.USER_AGENT));

            if (price > UNINTERESTING_PRICE) {

                bidPrice.set(price);
                compositeKey.setCityCode(cityCode);
                compositeKey.setOperationalSystem(os);
                ctx.write(compositeKey, bidPrice);
            }
        }
    }
}