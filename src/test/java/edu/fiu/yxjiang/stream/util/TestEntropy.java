package edu.fiu.yxjiang.stream.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import backtype.storm.tuple.Values;

public class TestEntropy {

	@Test
	public void testLeaveOneOutEntropy() {
		double[] data1 = {0.68, 0.67, 0.66, 0.65, 0.94, 0.63, 0.05};
		
		Map<Double, List<String>> histogram = new HashMap<Double, List<String>>();
		for(int i = 0; i < data1.length; ++i) {
			List<String> l = new ArrayList<String>();
			l.add("test");
			histogram.put(data1[i], l);
		}
		
		MaximumLikelihoodNormalDistribution mlnd = new MaximumLikelihoodNormalDistribution(data1.length, histogram);
		
		double entropy = Entropy.calculateEntropyNormalDistribution(mlnd.getSigma());
		
//		List<String> l = histogram.get(0.96);
//		l.clear();
//		
//		MaximumLikelihoodNormalDistribution mlnd2 = new MaximumLikelihoodNormalDistribution(data1.length - 1, histogram);
//		System.out.println(mlnd2.getMu());
		
		int group = 0;
		for(Map.Entry<Double, List<String>> entry : histogram.entrySet()) {
			//	calculate entropy difference one by one
			double value = entry.getKey();
			
			Map<Double, List<String>> leaveOneOutHist = new HashMap<Double, List<String>>();
			for(Map.Entry<Double, List<String>> originalEntry : histogram.entrySet()) {
				List<String> newEntityList = new ArrayList<String>();
				newEntityList.addAll(originalEntry.getValue());
				if(originalEntry.getKey() == value) {
					newEntityList.remove(0);
//					System.out.println("remove " + value);
				}
				leaveOneOutHist.put(originalEntry.getKey(), newEntityList);
			}
			
//			//	print histogram
//			System.out.println("\nHist:");
//			for(Map.Entry<Double, List<String>> copyEntry : leaveOneOutHist.entrySet()) {
//				System.out.println(copyEntry.getKey() + ":" + copyEntry.getValue());
//			}
//			System.out.println();
			
			MaximumLikelihoodNormalDistribution ml = new MaximumLikelihoodNormalDistribution(data1.length - 1, leaveOneOutHist);
			double leaveOneOutEntropy = Entropy.calculateEntropyNormalDistribution(ml.getSigma());
			double entropyReduce = entropy - leaveOneOutEntropy;
			entropyReduce = entropyReduce > 0? entropyReduce : 0;
			++group;
			
			String entityId = entry.getValue().get(0);
			String str = "count:" + group + "\t\tEntity: " + entityId + "\tScore:" + entropyReduce + "\t\tCPU idle:" + entry.getKey() + "\n";
			System.out.println(str);
		}
		
	}
}
