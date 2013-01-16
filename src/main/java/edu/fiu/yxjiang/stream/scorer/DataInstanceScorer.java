package edu.fiu.yxjiang.stream.scorer;

import java.util.List;

/**
 * 
 * @author yexijiang
 *
 */
public abstract class DataInstanceScorer<T> {
	
	/**
	 * Calculate the data instance anomaly score for given data instances.
	 * @param observationList
	 * @return
	 */
	public abstract List<ScorePackage> getScores(List<T> observationList);
	
}
