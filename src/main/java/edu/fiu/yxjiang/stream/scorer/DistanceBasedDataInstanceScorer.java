package edu.fiu.yxjiang.stream.scorer;

import java.util.ArrayList;
import java.util.List;

import sysmon.common.metadata.MachineMetadata;

/**
 * Calculate the data instance anomaly score based on its distance to the cluster center.
 * @author hongtai li
 *
 */
public class DistanceBasedDataInstanceScorer extends DataInstanceScorer<MachineMetadata> {

	@Override
	public List<ScorePackage> getScores(List<MachineMetadata> observationList) {
		List<ScorePackage> scorePackageList = new ArrayList<ScorePackage>();
		
		for(MachineMetadata metadata : observationList)	 {
			scorePackageList.add(new ScorePackage(metadata.getMachineIP(), 1.0, metadata));
		}
		
		return scorePackageList;
	}

}
