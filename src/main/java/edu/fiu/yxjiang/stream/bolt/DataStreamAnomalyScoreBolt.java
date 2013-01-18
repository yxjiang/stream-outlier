package edu.fiu.yxjiang.stream.bolt;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/**
 * DataStreamAnomalyScoreBolt keeps and update the stream anomaly score for each stream.
 * @author yexijiang
 *
 */
public class DataStreamAnomalyScoreBolt<T> extends BaseRichBolt{
	
	private Map<String, StreamProfile<T>> streamProfiles;
	private OutputCollector collector;
	private double lambda;
	private double factor;
	private long previousTimestamp;
	
	public DataStreamAnomalyScoreBolt() {
		this.factor = Math.pow(Math.E, -lambda);
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.lambda = Double.parseDouble(stormConf.get("lambda").toString());
		this.streamProfiles = new HashMap<String, StreamProfile<T>>();
		this.previousTimestamp = 0;
	}

	@Override
	public void execute(Tuple input) {
		
		long timestamp = input.getLong(2);
		
		if(timestamp > previousTimestamp) {
			
			print();
			
			previousTimestamp = timestamp;
		}
		
		String id = input.getString(0);
		StreamProfile profile = streamProfiles.get(id);
		if(profile == null) {
			profile = new StreamProfile<T>((T)input.getValue(3));
			streamProfiles.put(id, profile);
		}
		else {
			double dataInstanceAnomalyScore = input.getDouble(1);
			profile.streamAnomalyScore = profile.streamAnomalyScore * factor + dataInstanceAnomalyScore;
			profile.streamAnomalyScore = profile.streamAnomalyScore + dataInstanceAnomalyScore;
			streamProfiles.put(id, profile);
		}
		
		this.collector.ack(input);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "streamAnomalyScore", "timestamp", "observation"));		
	}
	
	private void print() {
		for(int i = 0; i < 15; ++i) {
			System.out.println();
		}
		
		for(Map.Entry<String, StreamProfile<T>> entry : streamProfiles.entrySet()) {
			System.out.println(entry.getKey() + "\t" + entry.getValue().streamAnomalyScore);
		}
		
		for(int i = 0; i < 15; ++i) {
			System.out.println();
		}
	}
	
	/**
	 * Keeps the profile of the stream.
	 * @author yexijiang
	 *
	 * @param <T>
	 */
	class StreamProfile<T> {
		double streamAnomalyScore;
		T currentDataInstance;
		
		public StreamProfile(T dataInstanceScore) {
			this.streamAnomalyScore = 0.0;
			this.currentDataInstance = dataInstanceScore;
		}
	}
	
}
