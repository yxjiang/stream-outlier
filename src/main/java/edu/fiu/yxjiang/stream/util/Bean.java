package edu.fiu.yxjiang.stream.util;

import java.io.Serializable;

import sysmon.common.metadata.MachineMetadata;

public class Bean implements Serializable{
	public long timestamp;
	public String id;
	public double score;
	public boolean isAbnormal;
	public MachineMetadata observation;
}
