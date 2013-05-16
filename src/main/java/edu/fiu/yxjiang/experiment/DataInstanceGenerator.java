package edu.fiu.yxjiang.experiment;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.math3.distribution.MultivariateNormalDistribution;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.UncorrelatedRandomVectorGenerator;
import org.apache.commons.math3.random.UniformRandomGenerator;
import org.apache.commons.math3.random.Well1024a;



/**
 * Generate multidimensional data with gaussian generator.
 * @author yexijiang
 *
 */
public class DataInstanceGenerator {

	public static void generate(String outputPath, int size, double errorRate, double[] mean, double[][] covariance) 
			throws IOException {
		//	write samples
		long sampleSize = Math.round(size * (1 - errorRate));
		MultivariateNormalDistribution distribution = new MultivariateNormalDistribution(mean, covariance);
		BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath));
		for (long i = 0; i < sampleSize; ++i) {
			double[] sample = distribution.sample();
			writer.write(join(",", sample));
		}
		
		//	write error samples
		long errorSampleSize = size - sampleSize;
		RandomGenerator rndGen = new Well1024a();
		UniformRandomGenerator uniGen = new UniformRandomGenerator(rndGen);
		UncorrelatedRandomVectorGenerator unRndGen = new UncorrelatedRandomVectorGenerator(mean.length, uniGen);
		for (long i = 0; i < size; ++i) {
			double[] sample = unRndGen.nextVector();
			writer.write(join(",", sample));
		}
		
		writer.close();
	}
	
	private static String join(String delimiter, double[] elem) {
		StringBuilder sb = new StringBuilder();
		if (elem != null && elem.length != 0) {
			sb.append(elem[0]);
			for (int i = 1; i < elem.length; ++i) {
				sb.append(delimiter);
				sb.append(elem[i]);
			}
			sb.append('\n');
		}
		
		return sb.toString();
	}
	
	public static void main(String[] args) throws IOException {
		int size = 2000;
		double errorRate = 0.1;
		double[] mean = {50, 20};
		double[][] covariance = {
				{1.0, 0.2},
				{0.2, 1.0}
		};
		String outputPath = "data/dataInstance-50-20.txt";
		DataInstanceGenerator.generate(outputPath, size, errorRate, mean, covariance);
	}
	
}
