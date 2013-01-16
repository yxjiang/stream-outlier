package edu.fiu.yxjiang.stream.bolt;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.task.OutputCollector;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class ObservationScoreBolt<T> extends BaseBasicBolt{

	private OutputCollector collector;
	private long previousTimestamp;
	private List<T> observationList;
	
	public ObservationScoreBolt() {
		this.previousTimestamp = 0;
		this.observationList = new ArrayList<T>();
	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		long currentTimestamp = input.getLong(0);	//	the first field is the timestamp
		if(currentTimestamp > previousTimestamp) {	
			//		a new batch of observations
			
		}
		
		
		
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("entityID", "timestamp", "dataInstanceScore"));		
	}

}
