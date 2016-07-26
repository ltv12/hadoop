package workers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import com.google.common.collect.Lists;

import utils.MapUtils;

/**
 * Created by Lev_Khacheresiantc on 7/25/2016.
 */
public class ReadWorker implements Callable<Integer> {

    private AtomicInteger tempFileId = new AtomicInteger(0);

    private HashMap<String, Integer> batch = new HashMap<>();

    private Path source;
    private Path target;
    private Configuration configuration;
    private int batchSize;

    public ReadWorker(Configuration configuration, Path source, Path target,
        int batchSize) {
        this.batchSize = batchSize;
        this.configuration = configuration;
        this.source = source;
        this.target = new Path(target, "tmp");
    }

    private static InputStream getStreamByCodecAndFile(FileSystem fileSystem,
        Path file, CompressionCodec codec) throws IOException {

        if (codec != null) {
            return codec.createInputStream(fileSystem.open(file));
        } else {
            return fileSystem.open(file);
        }
    }

    private static String getIPnoyoId(String line) throws IOException {
        return line.split("\\s")[2];
    }

    @Override
    public Integer call() {

        BufferedReader bufferedReader = null;
        FSDataOutputStream outputStream = null;

        try {
            FileSystem fileSystem = FileSystem.get(configuration);

            CompressionCodecFactory factory =
                new CompressionCodecFactory(configuration);

            CompressionCodec codec = factory.getCodec(source);

            InputStream inputStream = getStreamByCodecAndFile(
                FileSystem.newInstance(configuration), source, codec);
            bufferedReader =
                new BufferedReader(new InputStreamReader(inputStream));
            System.out.println("Started read " + source.toString());
            int count = 0;
            while (bufferedReader.ready()) {
                String iPinyouId = getIPnoyoId(bufferedReader.readLine());
                MapUtils.merge(batch, iPinyouId);
                count++;
                if (batchSize < count) {
                    Map<String, Integer> sortedMap =
                        MapUtils.getSortedMap(batch);

                    writeTempFile(FileSystem.newInstance(configuration),
                        sortedMap);
                    batch.clear();
                    count = 0;
                }
            }
            System.out.println("Started read " + source.toString());

        } catch (IOException ioe) {
            System.out.println("Error during reading file");
        } finally {
            IOUtils.closeStream(bufferedReader);
        }
        return 0;
    }

    private void writeTempFile(FileSystem fileSystem,
        Map<String, Integer> sortedMap) {
        FSDataOutputStream outputStream = null;
        try {
            outputStream = fileSystem.create(
                new Path(target, "tmp_" + tempFileId.incrementAndGet()),
                (short) 1);
            for (Map.Entry<String, Integer> entry : sortedMap.entrySet()) {
                outputStream
                        .writeBytes(entry.getKey() + " " + entry.getValue() + "\n");
            }
        } catch (IOException e) {
            System.out.println(
                Lists.asList(new StackTraceElement[1], e.getStackTrace()));
        }finally {
            IOUtils.closeStream(outputStream);
        }
    }
}
