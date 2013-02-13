package edu.fiu.yxjiang.stream.scorer;

public class DataInstanceScorerFactory {
	
	public static DataInstanceScorer getDataInstanceScorer(String dataTypeName) {
		if(dataTypeName.equalsIgnoreCase("computerMetaData")) {
			return new DistanceBasedDataInstanceScorer();
		}
		else if(dataTypeName.equals("twitterData")) {
			return new TwitterDataInstanceScorer();
		}
		try {
			throw new Exception("No matched data type scorer for " + dataTypeName);
		} catch(Exception e) {
			e.printStackTrace();
		}
		return null;
	}
}
