package edu.fiu.yxjiang.stream.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import edu.fiu.yxjiang.stream.scorer.DataInstanceScorer;
import edu.fiu.yxjiang.stream.scorer.DistanceBasedDataInstanceScorer;

public class ObservationScoreBolt extends BaseRichBolt{

	private long previousTimestamp;
	private OutputCollector collector;
	private DataInstanceScorer dataInstanceScorer;
	private List<Object> observationList;
	
	public ObservationScoreBolt() {
		this.previousTimestamp = 0;
		this.observationList = new ArrayList<Object>();
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.dataInstanceScorer = new DistanceBasedDataInstanceScorer();
	}

	@Override
	public void execute(Tuple input) {
		long timestamp = input.getLong(0);
		if(timestamp > previousTimestamp) {
			//	a new batch of observations
			dataInstanceScorer.calculateScores(this.collector, observationList);
			observationList.clear();
			previousTimestamp = timestamp;
		}
		else if (timestamp < previousTimestamp) {
			return;
		}
		observationList.add(input.getValue(2));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "score", "instance"));
	}
	
	public boolean isAutoAck(){
		return true;
	}

}
