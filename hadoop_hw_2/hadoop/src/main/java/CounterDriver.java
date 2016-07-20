package main.java;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.xerces.parsers.CachingParserPool;
import utils.MapUtils;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Lev_Khacheresiantc on 7/18/2016.
 */
public class CounterDriver {

    private static HashMap<String, Integer> cache = new HashMap<>();

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        CompressionCodecFactory factory = new CompressionCodecFactory(conf);

        Path filesLocation = new Path(args[0]);
        Path outputFile = new Path(args[1]);

        if (!fs.exists(filesLocation)) {
            System.out.print("Directory isn't exists");
            return;
        }

        FileSystem fileSystem = FileSystem.get(filesLocation.toUri(), conf);
        FileStatus[] files = fileSystem.listStatus(filesLocation);

        BufferedReader bufferedReader = null;
        FSDataOutputStream outputStream = null;

        try {
            for (FileStatus file : files) {

                CompressionCodec codec = factory.getCodec(file.getPath());

                InputStream inputStream =
                    getStreamByCodecAndFile(fileSystem, file, codec);
                bufferedReader =
                    new BufferedReader(new InputStreamReader(inputStream));

                while (bufferedReader.ready()) {
                    String iPinyouId = getIPnoyoId(bufferedReader.readLine());
                    MapUtils.merge(cache, iPinyouId);
                }
            }

            List<String> top100 = MapUtils.getTop100(cache);

            outputStream = fs.create(outputFile);

            for (String key : top100) {
                outputStream.writeUTF(key + " " + cache.get(key) + "\n");
            }

        } catch (IOException ioe) {
            System.out.println(ioe);
        } finally {
            IOUtils.closeStream(bufferedReader);
            IOUtils.closeStream(outputStream);
        }

    }

    private static String getIPnoyoId(String line) throws IOException {
        return line.split("\\s")[2];
    }

    private static InputStream getStreamByCodecAndFile(FileSystem fileSystem,
        FileStatus file, CompressionCodec codec) throws IOException {

        if (codec != null) {
            return codec.createInputStream(fileSystem.open(file.getPath()));
        } else {
            return fileSystem.open(file.getPath());
        }
    }

}
