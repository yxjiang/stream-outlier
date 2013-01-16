package edu.fiu.yxjiang.stream.bolt;

import java.util.HashMap;
import java.util.Map;

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
	
	public DataStreamAnomalyScoreBolt(double lambda) {
		this.lambda = lambda;
		this.factor = Math.pow(Math.E, -factor);
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.streamProfiles = new HashMap<String, StreamProfile<T>>();
	}

	@Override
	public void execute(Tuple input) {
		String id = input.getString(0);
		StreamProfile profile = streamProfiles.get(id);
		if(profile == null) {
			profile = new StreamProfile<T>((T)input.getValue(3));
		}
		else {
			double dataInstanceAnomalyScore = input.getDouble(1);
			profile.streamAnomalyScore = profile.streamAnomalyScore * factor + dataInstanceAnomalyScore;
		}
		
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "score", "timestamp"));		
	}
	
	public boolean isAutoAck(){
		return true;
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
