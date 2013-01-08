package edu.fiu.yxjiang.stream.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import sysmon.common.metadata.MachineMetadata;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import edu.fiu.yxjiang.stream.util.Entropy;
import edu.fiu.yxjiang.stream.util.MaximumLikelihoodNormalDistribution;

public class DataInstancesScoreBolt extends BaseRichBolt {

	private OutputCollector collector;
	private long previousTimestamp;
	private Map<Double, List<String>> histogram; // histogram for a batch of data
																								// instances.
	private int totalCountInBatch; // total count for a batch of data instances.

	public DataInstancesScoreBolt() {
		this.previousTimestamp = 0;
		histogram = new HashMap<Double, List<String>>();
		totalCountInBatch = 0;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		long curTimestamp = input.getLong(0);
		String machineIp = input.getString(1);
		if (curTimestamp != previousTimestamp && totalCountInBatch != 0) { // a new
																																				// batch
																																				// of
																																				// data
																																				// instances
			// score data instances of previous batch
			MaximumLikelihoodNormalDistribution mlnd = new MaximumLikelihoodNormalDistribution(
					totalCountInBatch, histogram);

			double minIdle = Double.MAX_VALUE;
			for (Double v : histogram.keySet()) {
				if (v < minIdle) {
					minIdle = v;
				}
			}

			double entropy = Entropy.calculateEntropyNormalDistribution(mlnd.getSigma());

			StringBuffer blankLines = new StringBuffer();
			for (int i = 0; i < 20; ++i) {
				blankLines.append("\n");
			}

			// emit to stream score bolt
			String output = "";
			Set<Double> keySet = histogram.keySet();
			for (double key : keySet) {
				List<String> entityList = histogram.get(key);
				String firstEntity = entityList.remove(0);

				// estimate parameters for leave-one-out histogram
				MaximumLikelihoodNormalDistribution ml = new MaximumLikelihoodNormalDistribution(
						totalCountInBatch - 1, histogram);
				double leaveOneOutEntropy = Entropy.calculateEntropyNormalDistribution(ml.getSigma());
				double entropyReduce = entropy - leaveOneOutEntropy;
				entropyReduce = entropyReduce > 0 ? entropyReduce : 0;
				double score = entropyReduce * totalCountInBatch;

				// put the removed one back to histogram
				entityList.add(firstEntity);

				for (String entityId : entityList) {
					collector.emit(new Values(entityId, curTimestamp, score));
					output += "\t\tEntity: " + entityId + "\tScore:" + score + "\t\tCPU idle:" + key + "\n";
				}

			}

//			System.out.println(blankLines.toString() + output + blankLines);

			histogram.clear();
			totalCountInBatch = 0;
			previousTimestamp = curTimestamp;
		}

		MachineMetadata machineMetaData = (MachineMetadata) input.getValue(2);
		double idleTime = machineMetaData.getCpu().getIdleTime();
		idleTime = idleTime > 0 ? idleTime / 100000 * 100000 : 0;
		List<String> instancesList = histogram.get(idleTime);
		if (instancesList == null) {
			instancesList = new ArrayList<String>();
		}
		instancesList.add(machineIp);
		histogram.put(idleTime, instancesList);
		++totalCountInBatch;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("entityID", "timestamp", "dataInstanceScore"));
	}

}
