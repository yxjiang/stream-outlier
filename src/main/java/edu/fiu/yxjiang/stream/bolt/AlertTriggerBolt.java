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
import edu.fiu.yxjiang.stream.util.BFPRT;

/**
 * The AlertTriggerBolt triggers an alert if a stream is identified as abnormal.
 * @author yexijiang
 *
 */
public class AlertTriggerBolt extends BaseRichBolt {
	
	private long previousTimestamp;
	private OutputCollector collector;
	private List<Tuple> streamList;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.previousTimestamp = 0;
		this.streamList = new ArrayList<Tuple>();
	}

	@Override
	public void execute(Tuple input) {
		long timestamp = input.getLong(2);
		if(timestamp > previousTimestamp) {
			//	new batch of stream scores
			List<Object> abnormalStreams = this.identifyAbnormalStreams();
			streamList.clear();
			
			//	generate alert message
		}
		else if(timestamp == previousTimestamp) {
			streamList.add(input);
		}
		
		this.collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("anomalyStream"));
	}
	
	/**
	 * Identify the abnormal streams.
	 * @return
	 */
	private List<Object> identifyAbnormalStreams() {
		List<Object> abnormalStreamList = new ArrayList<Object>();
		int medianIdx = (int)Math.round(streamList.size() / 2 + 0.5);
		Tuple medianTuple = BFPRT.bfprt(streamList, medianIdx);
		for(int i = medianIdx + 1; i < streamList.size(); ++i) {
			Tuple stream = streamList.get(i);
			if(stream.getLong(1) > 2 * medianTuple.getLong(1)) {
				abnormalStreamList.add(stream.getValue(3));
			}
		}
		return abnormalStreamList;
	}
	
}
