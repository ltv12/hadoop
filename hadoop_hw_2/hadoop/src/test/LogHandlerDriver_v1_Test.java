import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import map.IpInfoMapper_v1;
import reduce.IpInfoReduce_v1;

/**
 * Created by lev on 17.07.16.
 */
public class LogHandlerDriver_v1_Test {

    MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;
    MapDriver<LongWritable, Text, Text, Text> mapDriver;
    ReduceDriver<Text, Text, Text, Text> reduceDriver;

    @Before
    public void setUp() {
        Mapper mapper = new IpInfoMapper_v1();
        Reducer reducer = new IpInfoReduce_v1();
        mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
        mapDriver.setMapper(mapper);
        reduceDriver = new ReduceDriver<Text, Text, Text, Text>();
        reduceDriver.setReducer(reducer);
        mapReduceDriver =
            new MapReduceDriver<LongWritable, Text, Text, Text, Text, Text>();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);
    }

    @Test
    public void testMapper() throws Exception {

        Text input1 = new Text(
            "ip32 - - [26/Apr/2011:21:24:44 -0400] \"GET /sun_lx/lx_bottom.jpg HTTP/1.1\""
                + " 200 13297 \"http://host3/sun_lx/\" \"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_7; en-US)"
                + " AppleWebKit/534.16 (KHTML, like Gecko) Chrome/10.0.648.205 Safari/534.16");
        Text input2 = new Text(
            "ip754 - - [26/Apr/2011:02:20:50 -0400] \"HEAD / HTTP/1.1\" 200 0 \"-\" \"urlresolver\"");

        mapDriver.withInput(new LongWritable(1), input1);
        mapDriver.withInput(new LongWritable(2), input2);
        mapDriver.withOutput(new Text("ip32"), new Text("1\t13297"));
        mapDriver.withOutput(new Text("ip754"), new Text("1\t0"));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<Text> values = Arrays.asList(new Text("1\t501"),
            new Text("1\t501"), new Text("1\t500"), new Text("1\t500"));

        reduceDriver.withInput(new Text("ip32"), values);
        reduceDriver.withOutput(new Text("ip32"), new Text("500.50,2002"));
        reduceDriver.runTest();

    }

    @Test
    public void testMapReducer() throws IOException {
        mapReduceDriver.withInput(new LongWritable(1), new Text(
            "ip754 - - [26/Apr/2011:02:20:50 -0400] \"HEAD / HTTP/1.1\" 200 0 \"-\" \"urlresolver\""));
        mapReduceDriver.withInput(new LongWritable(1), new Text(
            "ip754 - - [26/Apr/2011:02:20:50 -0400] \"HEAD / HTTP/1.1\" 200 49 \"-\" \"urlresolver\""));
        mapReduceDriver.addOutput(new Text("ip754"), new Text("24.50,49"));
        mapReduceDriver.runTest();

    }
}