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
import edu.fiu.yxjiang.stream.scorer.DataInstanceScorerFactory;

public class ObservationScoreBolt extends BaseRichBolt{

	private long previousTimestamp;
	private String dataTypeName;
	private OutputCollector collector;
	private DataInstanceScorer dataInstanceScorer;
	private List<Object> observationList;
	
	public ObservationScoreBolt(String dataTypeName) {
		this.previousTimestamp = 0;
		this.dataTypeName = dataTypeName;
		this.observationList = new ArrayList<Object>();
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.dataInstanceScorer = DataInstanceScorerFactory.getDataInstanceScorer(dataTypeName);
	}

	@Override
	public void execute(Tuple input) {
		long timestamp = input.getLong(0);
		if(timestamp > previousTimestamp) {
			//	a new batch of observation, calculate the scores of old batch and then emit 
			dataInstanceScorer.calculateScores(this.collector, observationList);
			observationList.clear();
			previousTimestamp = timestamp;
		}
		else if (timestamp == previousTimestamp) {
			observationList.add(input.getValue(2));
		}
		
		this.collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "dataInstanceAnomalyScore", "timestamp", "observation"));
	}
	
}
