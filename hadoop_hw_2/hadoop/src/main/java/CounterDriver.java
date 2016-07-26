import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import IdAndCount.IdAndCount;
import com.google.common.collect.Collections2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.hadoop.io.IOUtils;
import utils.ReaderUtils;
import workers.ReadWorker;

/**
 * Created by Lev_Khacheresiantc on 7/18/2016.
 */
public class CounterDriver {

    private static final int BATCH_SIZE = 1_000_000;

    private static HashMap<String, Integer> cache = new HashMap<>();

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path filesLocation = new Path(args[0]);
        Path outputFile = new Path(args[1]);
        Path tmp = new Path(outputFile, "tmp");
        Path target = new Path(outputFile, "result");

        if (!fs.exists(filesLocation)) {
            System.out.print("Directory isn't exists");
            return;
        }
        BufferedReader bufferedReader = null;

        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setNameFormat("Readers-%d").setDaemon(true).build();

        FileStatus[] files = fs.listStatus(filesLocation);

        final ExecutorService executorService =
            Executors.newFixedThreadPool(4, threadFactory);

        List<Callable<Integer>> readers = new ArrayList<>();
        for (FileStatus file : files) {
            readers.add(
                new ReadWorker(conf, file.getPath(), outputFile, BATCH_SIZE));
        }
        executorService.invokeAll(readers);
        List<BufferedReader> buffReaders = new ArrayList<BufferedReader>();
        List<IdAndCount> buffIds = new ArrayList<IdAndCount>();

        FileStatus[] tmpFiles = fs.listStatus(tmp);
        for (FileStatus file : tmpFiles) {
            buffReaders.add(new BufferedReader(
                new InputStreamReader(fs.open(file.getPath()))));
        }
        for (BufferedReader reader : buffReaders) {
            buffIds.add(ReaderUtils.readNextObject(reader));
        }

        IdAndCount currentId =
            Collections.max(buffIds, IdAndCount.COMPARATOR_BY_ID);

        final List<IdAndCount> temp = new ArrayList<>();
        final List<IdAndCount> top100 = new ArrayList<>(101);

        int index = 0;
        BufferedReader reader;

        while (buffReaders.size() > 0) {
            // System.out.println("Buffered list of ids " + buffIds);
            index = buffIds.indexOf(currentId);
            // System.out.println("index = " + index + ", id = " + currentId);

            if (index != -1) {
                currentId = buffIds.get(index);
                reader = buffReaders.get(index);
                IdAndCount nextOjbect = ReaderUtils.readNextObject(reader);

                if (nextOjbect == null) {
                    System.out.println("close reader " + index);
                    IOUtils.closeStream(reader);
                    buffIds.remove(index);
                    buffReaders.remove(index);

                } else {
                    buffIds.set(index, nextOjbect);
                    // System.out
                    // .println("change buff id " + nextOjbect.toString());
                    temp.add(currentId);

                }

            } else {
                // System.out.println("calculate value for " + currentId);
                // System.out.println("temp for " + currentId + ": " + temp);
                addToTop100(top100,
                    getIdWithTotalCount(temp, currentId.getId()));
                currentId =
                    Collections.max(buffIds, IdAndCount.COMPARATOR_BY_ID);

                if (top100.size() > 2) {
                    break;
                }
            }
        }
        Collections.sort(top100, IdAndCount.COMPARATOR_BY_COUNT);
        Collections.reverse(top100);

        fs.delete(tmp, true);
        FSDataOutputStream outputStream = fs.create(target);

        for (IdAndCount item : top100) {
            outputStream.writeBytes(item.toString() + "\n");
        }
        IOUtils.closeStream(outputStream);

    }

    public static IdAndCount getIdWithTotalCount(List<IdAndCount> temp,
        String id) {
        IdAndCount idAndTotalCount = new IdAndCount(id, 0);

        for (IdAndCount item : temp) {
            idAndTotalCount
                .setCount(idAndTotalCount.getCount() + item.getCount());
        }
        temp.clear();
        return idAndTotalCount;
    }

    public static void addToTop100(List<IdAndCount> top100, IdAndCount newId) {
        if (top100.size() < 100) {
            top100.add(newId);
        } else {
            IdAndCount min =
                Collections.min(top100, IdAndCount.COMPARATOR_BY_COUNT);
            if (newId.getCount() > min.getCount()) {
                top100.remove(min);
                top100.add(newId);
            }
        }
    }

    /*
     * outputStream = fs.create(outputFile);
     * 
     * for (String key : top100) { outputStream.writeChars(key + " " +
     * cache.get(key) + "\n"); }
     */

}
