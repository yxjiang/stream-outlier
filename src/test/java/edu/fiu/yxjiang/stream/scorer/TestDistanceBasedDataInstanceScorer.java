package edu.fiu.yxjiang.stream.scorer;

import org.junit.Before;
import org.junit.Test;

public class TestDistanceBasedDataInstanceScorer {

	private double[][] matrix;
	
	@Before
	public void before() {
		matrix = new double[][] {
				{0.88, 82},
				{0.97, 86},
				{0.95, 87},
				{0.96, 86},
				{0.97, 86},
				{0.97, 86},
				{0.95, 8}
		};
		
	}
	
	@Test
	public void testCalculateDistances() {
		DistanceBasedDataInstanceScorer scorer = new DistanceBasedDataInstanceScorer();
		double[] distances = scorer.calculateDistance(matrix);
		for(int i = 0; i < distances.length; ++i) {
			double distance = distances[i];
			System.out.print("(");
			for(int j = 0; j < matrix[i].length; ++j) {
				System.out.print(matrix[i][j]);
				if(j != matrix[i].length - 1) {
					System.out.print(", ");
				}
			}
			System.out.print(")");
			
			System.out.println(":\t" + distance);
		}
	}
	
}
