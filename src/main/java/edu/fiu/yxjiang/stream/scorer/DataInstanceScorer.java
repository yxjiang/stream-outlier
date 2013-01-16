package edu.fiu.yxjiang.stream.scorer;

import java.util.List;

/**
 * DataInstanceScorer defines the method to calculate the data instance anomaly scores.
 * @author yexijiang
 *
 */
public abstract class DataInstanceScorer<T> {
	
	/**
	 * Calculate the data instance anomaly score for given data instances.
	 * @param observationList
	 * @return
	 */
	public abstract List<ScorePackage<T>> getScores(List<T> observationList);
	
}
