package edu.fiu.yxjiang.stream.scorer;

import java.util.List;

import backtype.storm.task.OutputCollector;

/**
 * DataInstanceScorer defines the method to calculate the data instance anomaly scores.
 * @author yexijiang
 *
 */
public abstract class DataInstanceScorer<T> {
	
	/**
	 * Calculate the data instance anomaly score for given data instances and directly send to downstream.
	 * @param collector
	 * @param observationList
	 */
	public abstract void calculateScores(OutputCollector collector, List<T> observationList);
	
}
