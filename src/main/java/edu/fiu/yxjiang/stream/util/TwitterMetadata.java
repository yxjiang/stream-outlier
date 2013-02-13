package edu.fiu.yxjiang.stream.util;

import java.io.Serializable;


public class TwitterMetadata implements Serializable{
	
	private String topic;
	private int count;
	
	public TwitterMetadata(String topic, int count) {
		this.topic = topic;
		this.count = count;
	}

	public String getTopic() {
		return topic;
	}
	
	public void setTopic(String topic) {
		this.topic = topic;
	}
	
	public int getChangeRatio() {
		return count;
	}
	
	public void setCount(int count) {
		this.count = count;
	}
	
}
