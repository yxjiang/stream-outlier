package edu.fiu.yxjiang.stream.scorer;

import java.util.ArrayList;
import java.util.List;

import edu.fiu.yxjiang.stream.util.TwitterMetadata;

public class TwitterDataInstanceScorer extends DataInstanceScorer<TwitterMetadata>{

	@Override
	public List<ScorePackage> getScores(List<TwitterMetadata> observationList) {
		//	measure the observation to the center
		List<ScorePackage> scorePackageList = new ArrayList<ScorePackage>();
		
		float center = 0.0f;
		int maxRatio = 500;
		int minRatio = 0;
		for(int i = 0; i < observationList.size(); ++i) {
			TwitterMetadata twitterMetadata = observationList.get(i);
			int ratio = twitterMetadata.getChangeRatio();
			center += ratio;
//			if(ratio > maxRatio) {
//				maxRatio = ratio;
//			}
//			if(ratio < minRatio) {
//				minRatio = ratio;
//			}
		}
		center /= observationList.size();
		//	normalize
		center = (center - minRatio) / (maxRatio - minRatio);
		
		for(int i = 0; i < observationList.size(); ++i) {
			float normalizedRatio = (observationList.get(i).getChangeRatio() - minRatio) / (maxRatio - minRatio);
			float dist = Math.abs(center - normalizedRatio);
			ScorePackage scorePackage = new ScorePackage(observationList.get(i).getTopic(), dist, observationList.get(i));
			scorePackageList.add(scorePackage);
		}
		
		return scorePackageList;
	}

}
