package edu.fiu.yxjiang.stream.scorer;

import java.util.ArrayList;
import java.util.Arrays;
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
		
		double[][] matrix = new double[observationList.size()][2];
		int colNumber = matrix[0].length;
		
		for(int i = 0; i < observationList.size(); ++i)	 {
			MachineMetadata metadata = observationList.get(i);
			scorePackageList.add(new ScorePackage(metadata.getMachineIP(), 1.0, metadata));
			matrix[i][0] = metadata.getCpu().getIdleTime();
			matrix[i][1] = metadata.getMemory().getFreePercent();
		}
		
		double[] mins = new double[matrix[0].length];
		double[] maxs = new double[matrix[0].length];
		
		//	find min and max for each column
		for(int col = 0; col < colNumber; ++col) {
			double min = Double.MAX_VALUE;
			double max = Double.MIN_VALUE;
			for(int row = 0; row < matrix.length; ++row) {
				if(matrix[row][col] > min) {
					min = matrix[row][col];
					max = matrix[row][col];
				}
			}
			mins[col] = min;
			maxs[col] = max;
		}
		
		//	min-max normalization by column
		double[] centers = new double[colNumber];
		Arrays.fill(centers, 0);
		for(int col = 0; col < colNumber; ++col) {
			if(mins[col] == 0 && maxs[col] == 0) {
				continue;
			}
			for(int row = 0; row < matrix.length; ++row) {
				matrix[row][col] = (matrix[row][col] - mins[col]) / (maxs[col] - mins[col]);
				centers[col] += matrix[row][col];
			}
			centers[col] /= matrix.length;
		}
		
		
		
		
		return scorePackageList;
	}

}
