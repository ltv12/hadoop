package map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import customtypes.IpInfoWritable;
import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.UserAgent;
import utils.LogParseUtils;

/**
 * Created by Lev_Khacheresiantc on 7/13/2016.
 */
public class IpInfoMapper_v2
    extends Mapper<LongWritable, Text, Text, IpInfoWritable> {

    private Text ip = new Text();
    private IpInfoWritable info = new IpInfoWritable(0.0, 0);

    public void map(LongWritable docId, Text text, Context ctx)
        throws IOException, InterruptedException {

        String[] data = text.toString().split("\\s+");

        String ipStr = LogParseUtils.getIp(data);

        int totalBytes = 0;
        if (ipStr != null && ipStr.startsWith("ip")) {
            ip.set(ipStr);

            totalBytes = LogParseUtils.getBytes(data);
            info.setTotalBytes(totalBytes);

            Browser browser =
                UserAgent.parseUserAgentString(text.toString()).getBrowser();

            ctx.write(ip, info);
            if (browser != null) {
                ctx.getCounter("Browsers", browser.getName()).increment(1);
            }
        }
    }
}
