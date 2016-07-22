package map;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.UserAgent;
import utils.LogParseUtils;

/**
 * Created by Lev_Khacheresiantc on 7/13/2016.
 */
public class IpInfoMapper_v1
    extends Mapper<LongWritable, Text, Text, Text> {

    private Text ip = new Text();
    private Text totalCountAndBytes = new Text();

    public void map(LongWritable docId, Text text, Context ctx)
        throws IOException, InterruptedException {
        String[] data = text.toString().split("\\s+");
        String ipStr = LogParseUtils.getIp(data);

        if (ipStr != null && ipStr.startsWith("ip")) {
            ip.set(ipStr);
            totalCountAndBytes.set("1\t" + LogParseUtils.getBytes(data));

            Browser browser =
                UserAgent.parseUserAgentString(text.toString()).getBrowser();

            ctx.write(ip, totalCountAndBytes);
            if (browser != null) {
                ctx.getCounter("Browsers", browser.getName()).increment(1L);
            }
        }
    }
}
