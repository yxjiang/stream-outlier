package edu.fiu.yxjiang.stream.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.junit.Ignore;
import org.junit.Test;

public class TestMaximumLikelihoodNormalDistribution {

	@Ignore
	@Test
	public void testMaximumLikelihoodNormalDistribution() {
		double[] data1 = {0.98, 0.97, 0.96, 0.95, 0.94, 0.93, 0.05};
		double[] data2 = {0.78, 0.77, 0.76, 0.75, 0.74, 0.73, 0.05};
		Map<Double, List<String>> histogram1 = new HashMap<Double, List<String>>();
		Map<Double, List<String>> histogram2 = new HashMap<Double, List<String>>();
		for(int i = 0; i < data1.length; ++i) {
			List l = new ArrayList<String>();
			l.add("test");
			histogram1.put(data1[i], l);
			histogram2.put(data2[i], l);
		}
		
		
		
		MaximumLikelihoodNormalDistribution ml1 = new MaximumLikelihoodNormalDistribution(data1.length, histogram1);
		System.out.println("Mean1:" + ml1.getMu());
		System.out.println("Sigma1:" + ml1.getSigma());
		
		StandardDeviation sd = new StandardDeviation();
		double sdv = sd.evaluate(data1);
		System.out.println("Standard deviation1:" + sdv);
		
		
		MaximumLikelihoodNormalDistribution ml2 = new MaximumLikelihoodNormalDistribution(data2.length, histogram2);
		System.out.println("Mean2:" + ml2.getMu());
		System.out.println("Sigma2:" + ml2.getSigma());
		
		double sdv2 = sd.evaluate(data2);
		System.out.println("Standard deviation2:" + sdv2);
	}
	
}
