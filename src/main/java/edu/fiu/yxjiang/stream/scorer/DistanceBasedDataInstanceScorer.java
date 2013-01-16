package edu.fiu.yxjiang.stream.scorer;

import java.util.List;

import sysmon.common.metadata.MachineMetadata;

/**
 * Calculate the data instance anomaly score based on its distance to the cluster center.
 * @author hongtai li
 *
 */
public class DistanceBasedDataInstanceScorer extends DataInstanceScorer<MachineMetadata> {

	@Override
	public List<ScorePackage<MachineMetadata>> getScores(List<MachineMetadata> observationList) {
		return null;
	}

}
