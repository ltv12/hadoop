package com.hd.form;

import javax.validation.constraints.Size;

public class ConfigurationForm {

	@Size(min = 128)
	private Integer memory;

	@Size(min = 1)
	private Integer cores;

	public Integer getPriority() {
		return priority;
	}

	public void setPriority(Integer priority) {
		this.priority = priority;
	}

	@Size(min = 1, max = 10)
	private Integer priority;

	@Size(min = 1)
	private Integer numberOfContainers;

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

	public Integer getConainersNumber() {
		return numberOfContainers;
	}

	public void setNumberOfContainers(Integer numberOfContainers) {
		this.numberOfContainers = numberOfContainers;
	}
}