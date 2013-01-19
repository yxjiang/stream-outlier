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
import backtype.storm.tuple.Values;
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
			if(streamList.size() != 0) {
				List<Tuple> abnormalStreams = this.identifyAbnormalStreams();
				for(Tuple abnormal : abnormalStreams) {
					collector.emit(new Values(abnormal.getString(0), abnormal.getDouble(1)));
				}
				this.streamList.clear();
			}
			
			this.previousTimestamp = timestamp;
		}
		
		this.streamList.add(input);
		this.collector.ack(input);
	}

//	@Override
//	public void execute(Tuple input) {
//		long timestamp = input.getLong(2);
//		if(timestamp > previousTimestamp) {
//			if(streamList.size() != 0) {
//				String message = this.identifyAbnormalStreamsTest();
//				this.collector.emit(new Values(message));
//				this.streamList.clear();
//			}
//			this.previousTimestamp = timestamp;
//		}
//		
//		this.streamList.add(input);
//		this.collector.ack(input);
//	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("anomalyStream", "streamAnomalyScore"));
//		declarer.declare(new Fields("message"));
	}
	
	/**
	 * Identify the abnormal streams.
	 * @return
	 */
	private List<Tuple> identifyAbnormalStreams() {
		List<Tuple> abnormalStreamList = new ArrayList<Tuple>();
		
		double minScore = Double.MAX_VALUE;
		for(int i = 0; i < streamList.size(); ++i) {
			Tuple tuple = streamList.get(i);
			if(minScore > tuple.getDouble(1)) {
				minScore = tuple.getDouble(1);
			}
		}
		
		int medianIdx = (int)Math.round(streamList.size() / 2);
		Tuple medianTuple = BFPRT.bfprt(streamList, medianIdx);
		
		for(int i = medianIdx + 1; i < streamList.size(); ++i) {
			Tuple stream = streamList.get(i);
			if(stream.getDouble(1) - medianTuple.getDouble(1) > medianTuple.getDouble(1) - minScore) {
				abnormalStreamList.add(stream);
			}
		}
		
		return abnormalStreamList;
	}
	
	
	private String identifyAbnormalStreamsTest() {
		String message = "";
		List<Tuple> abnormalStreamList = new ArrayList<Tuple>();
		
		double minScore = Double.MAX_VALUE;
		for(int i = 0; i < streamList.size(); ++i) {
			Tuple tuple = streamList.get(i);
			if(minScore > tuple.getDouble(1)) {
				minScore = tuple.getDouble(1);
			}
		}
		
		int medianIdx = (int)Math.round(streamList.size() / 2);
		Tuple medianTuple = BFPRT.bfprt(streamList, medianIdx);
		
		for(int i = medianIdx + 1; i < streamList.size(); ++i) {
			Tuple streamTuple = streamList.get(i);
			message += streamTuple.getString(0) + ":" + streamTuple.getDouble(1);
			if(streamTuple.getDouble(1) == medianTuple.getDouble(1)) {
				message += "(Median)\n";
			}
			else if(streamTuple.getDouble(1) - medianTuple.getDouble(1) > medianTuple.getDouble(1) - minScore) {
				message += "(Outlier)\n";
			}
			else if(streamTuple.getDouble(1) == minScore) {
				message += "(Min)\n";
			}
			else {
				message += "\n";
			}
		}
	
		return message;
	}
	
}
