package edu.fiu.yxjiang.stream.scorer;

/**
 * ScorePackage contains the score as well as the object.
 * @author yexijiang
 *
 * @param <T>
 */
public class ScorePackage<T> {
	
	private double score;
	private T obj;
	
	public ScorePackage(double score, T obj) {
		super();
		this.score = score;
		this.obj = obj;
	}

	public double getScore() {
		return score;
	}
	
	public void setScore(double score) {
		this.score = score;
	}
	
	public T getObj() {
		return obj;
	}
	
	public void setObj(T obj) {
		this.obj = obj;
	}
	
}
