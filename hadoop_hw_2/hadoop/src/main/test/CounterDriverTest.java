import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import comporator.CompositeKeyWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import map.BidCountMapper;
import reduce.BidCountReduce;

/**
 * Created by lev on 17.07.16.
 */
public class CounterDriverTest {

    MapReduceDriver<LongWritable, Text, CompositeKeyWritable, LongWritable, Text, LongWritable> mapReduceDriver;
    MapDriver<LongWritable, Text, CompositeKeyWritable, LongWritable> mapDriver;
    ReduceDriver<CompositeKeyWritable, LongWritable, Text, LongWritable> reduceDriver;

    @Before
    public void setUp() {
        Mapper mapper = new BidCountMapper();
        Reducer reducer = new BidCountReduce();
        mapDriver = new MapDriver<>();
        mapDriver.setMapper(mapper);
        reduceDriver = new ReduceDriver<>();
        reduceDriver.setReducer(reducer);
        mapReduceDriver = new MapReduceDriver<>();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);
    }

    @Test
    public void testMapper() throws Exception {

        Text input1 = new Text(
            "dbe06f7b386d619d3ab4dde3c93ed6d3\t20131020015400898\t1\tC6G2Ng4C2m\tOpera/9.80 (Windows NT 5.1; Edition IBIS) Presto/2.12.388 Version/12.10\t14.126.118.*\t216\t"
                + "216\t2\tbe2441d0219febc9149a807c39bde186\ta763717a9f8d613708311ed70940fa7a\tnull\t2759127038\t200\t200\tOtherView\tNa\t5\t7319\t"
                + "277\t37\tnull\t2259\t10057,10059,14273,13866,10006,10110,10031,10052,10127,10063,10116");
        Text input2 = new Text(
            "38097e8e6f3f18822ca63b3f332f1a95\t20131020122600477\t1\tCB1EZLCKbF6\tMozilla/5.0 (Windows NT 6.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1\t116.18.45.*\t216\t"
                + "233\t2\tcdc06a6ba07571f4b842a14390f861a\t2994d07cac4b56c4936a3a9ea0552cc4\tnull\t2894521600\t336\t280\tOtherView\tNa\t23\t7326\t"
                + "175\t45\tnull\t2259\t10057,10059,14273,10079,10077,10075,10006,10024,10110,10146,10052,13403,10063,10116,10125");

        mapDriver.withInput(new LongWritable(1), input1);
        mapDriver.withInput(new LongWritable(2), input2);
        mapDriver.withOutput(new CompositeKeyWritable(216, "Windows"),
            new LongWritable(277));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<LongWritable> values = Arrays.asList(new LongWritable(555),
            new LongWritable(500), new LongWritable(445));
        reduceDriver.addCacheFile(
            "D:\\work\\projects\\hw-reports\\cached\\city.en.txt");

        reduceDriver.withInput(new CompositeKeyWritable(0, "xxx"), values);
        reduceDriver.withOutput(new Text("unknown"), new LongWritable(1500));
        reduceDriver.runTest();

    }

    @Test
    public void testMapReducer() throws IOException {

        Text input1 = new Text(
            "dbe06f7b386d619d3ab4dde3c93ed6d3\t20131020015400898\t1\tC6G2Ng4C2m\tOpera/9.80 (Windows NT 5.1; Edition IBIS) Presto/2.12.388 Version/12.10\t14.126.118.*\t216\t"
                + "216\t2\tbe2441d0219febc9149a807c39bde186\ta763717a9f8d613708311ed70940fa7a\tnull\t2759127038\t200\t200\tOtherView\tNa\t5\t7319\t"
                + "277\t37\tnull\t2259\t10057,10059,14273,13866,10006,10110,10031,10052,10127,10063,10116");
        Text input2 = new Text(
            "38097e8e6f3f18822ca63b3f332f1a95\t20131020122600477\t1\tCB1EZLCKbF6\tMozilla/5.0 (Windows NT 6.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1\t116.18.45.*\t216\t"
                + "233\t2\tcdc06a6ba07571f4b842a14390f861a\t2994d07cac4b56c4936a3a9ea0552cc4\tnull\t2894521600\t336\t280\tOtherView\tNa\t23\t7326\t"
                + "175\t45\tnull\t2259\t10057,10059,14273,10079,10077,10075,10006,10024,10110,10146,10052,13403,10063,10116,10125");

        mapReduceDriver.withInput(new LongWritable(1), input1);
        mapReduceDriver.withInput(new LongWritable(1), input2);
        mapReduceDriver.addOutput(new Text("216"), new LongWritable(277));
        mapReduceDriver.runTest();

    }
}