package com.hd.form;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.Size;

public class ConfigurationForm {

    @Min(128)
    @Max(4096)
    private Integer memory;
    @Min(1)
    @Max(4)
    private Integer cores;
    @Min(1)
    @Max(3)
    private Integer priority;
    @Min(1)
    @Max(5)
    private Integer numberOfContainers;

    public ConfigurationForm() {
    }

    private ConfigurationForm(Integer memory, Integer cores, Integer priority,
        Integer numberOfContainers) {
        this.memory = memory;
        this.cores = cores;
        this.priority = priority;
        this.numberOfContainers = numberOfContainers;
    }

    public static ConfigurationForm createDefault() {
        return new ConfigurationForm(512, 1, 1, 1);
    }

    public Integer getPriority() {
        return priority;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
    }

    public Integer getMemory() {
        return memory;
    }

    public void setMemory(Integer memory) {
        this.memory = memory;
    }

    public Integer getCores() {
        return cores;
    }

    public void setCores(Integer cores) {
        this.cores = cores;
    }

    public Integer getNumberOfContainers() {
        return numberOfContainers;
    }

    public void setNumberOfContainers(Integer numberOfContainers) {
        this.numberOfContainers = numberOfContainers;
    }
}