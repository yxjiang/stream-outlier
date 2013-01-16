package edu.fiu.yxjiang.stream.scorer;

public class DataInstanceScorerFactory {
	
	public static DataInstanceScorer getDataInstanceScorer(String dataTypeName) {
		if(dataTypeName.equalsIgnoreCase("machineMetaData")) {
			return new DistanceBasedDataInstanceScorer();
		}
		
		return null;
	}
}
