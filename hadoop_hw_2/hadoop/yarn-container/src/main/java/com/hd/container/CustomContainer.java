package com.hd.container;

import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.yarn.annotation.OnContainerStart;
import org.springframework.yarn.annotation.YarnComponent;

@YarnComponent
public class CustomContainer {

    private static final int LIMIT = 10000000;
    private static Logger LOG = LoggerFactory.getLogger(CustomContainer.class);
    private static String RESULT_PATH = "/hw/hw-5/";

    @Autowired
    private Configuration configuration;

    @OnContainerStart
    public void onContainerStart() throws Exception {
        LOG.info("STEP-1: Init data for sorting");
        Random rand = new Random();
        Queue<Integer> randomInts = new LinkedList<Integer>();
        while (randomInts.size() < LIMIT) {
            randomInts.add(rand.nextInt(100000));
        }

        LOG.info("STEP-2: Sorting data for sorting");
        Set<Integer> sortedSet = new TreeSet<Integer>(randomInts);
        List<Integer> temp = new ArrayList<Integer>(sortedSet);
        Collections.reverse(temp);
        List<Integer> top100 = temp.subList(0, 100);

        LOG.info("STEP-3: Writing data to HDFS");
        FileSystem fileSystem = FileSystem.get(configuration);
        Path targetFolder =
            new Path(RESULT_PATH + UUID.randomUUID().toString());

        FSDataOutputStream outputStream = fileSystem.create(targetFolder);

        for (Integer integer : top100) {
            outputStream.writeBytes(integer + "\n");
        }

        IOUtils.closeStream(outputStream);
        LOG.info("STEP-4: release resources");

    }

}
