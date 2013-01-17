package edu.fiu.yxjiang.stream.scorer;

import java.util.List;

import sysmon.common.metadata.MachineMetadata;
import backtype.storm.task.OutputCollector;

/**
 * Calculate the data instance anomaly score based on its distance to the cluster center.
 * @author hongtai li
 *
 */
public class DistanceBasedDataInstanceScorer extends DataInstanceScorer<MachineMetadata> {

	@Override
	public List<ScorePackage> getScores(List<MachineMetadata> observationList) {
		// TODO Auto-generated method stub
		return null;
	}

}
