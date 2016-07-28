package com.hd.master;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.yarn.am.AbstractEventingAppmaster;
import org.springframework.yarn.am.YarnAppmaster;
import org.springframework.yarn.am.allocate.AbstractAllocator;
import org.springframework.yarn.am.allocate.ContainerAllocateData;
import org.springframework.yarn.am.allocate.DefaultContainerAllocator;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Created by Lev_Khacheresiantc on 7/26/2016.
 */
public class CustomApplicationMaster extends AbstractEventingAppmaster
    implements YarnAppmaster {

    public static Logger LOG =
        LoggerFactory.getLogger(CustomApplicationMaster.class);

    private int memory;

    @Override
    protected void onInit() throws Exception {
        super.onInit();
    }

    @Override
    protected void onContainerAllocated(Container container) {
        List<String> commands = new ArrayList<String>(getCommands());
        commands.add("-Xmx" + memory + "m");
        commands.add("-Xms" + memory + "m");
        getLauncher().launchContainer(container, commands);
    }

    @Override
    public void submitApplication() {
        registerAppmaster();
        start();
        if (getAllocator() instanceof AbstractAllocator) {
            ((AbstractAllocator) getAllocator())
                .setApplicationAttemptId(getApplicationAttemptId());
        }
    }

    public void runApplication(int memory, int priority, int cores,
        int conteinersCnt) {

        this.memory = memory;

        LOG.info(
            "Running {} containers [memory: {}, priority:{}, containers:{}]",
            conteinersCnt, memory, priority);

        DefaultContainerAllocator allocator =
            (DefaultContainerAllocator) getAllocator();
        allocator.setMemory(memory);
        allocator.setPriority(priority);

        ContainerAllocateData data = new ContainerAllocateData();
        data.addAny(conteinersCnt);
        String allocationGroupId = UUID.randomUUID().toString();
        data.setId(allocationGroupId);

        allocator.setAllocationValues(allocationGroupId, priority, null, cores,
            memory, false);
        allocator.allocateContainers(data);


    }
}
