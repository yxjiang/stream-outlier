package edu.fiu.yxjiang.stream.scorer;

import org.junit.Before;
import org.junit.Test;

public class TestDistanceBasedDataInstanceScorer {

	private double[][] matrix;
	
	@Before
	public void before() {
		matrix = new double[][] {
				{22, 0.8},
				{21, 0.6},
				{15, 0.6},
				{31, 0.9},
		};
		
	}
	
	@Test
	public void testCalculateDistances() {
		DistanceBasedDataInstanceScorer scorer = new DistanceBasedDataInstanceScorer();
		double[] distances = scorer.calculateDistance(matrix);
		
	}
	
}
