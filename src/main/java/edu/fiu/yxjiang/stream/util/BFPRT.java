package edu.fiu.yxjiang.stream.util;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.tuple.Tuple;

/**
 * Implementation of BRPRT algorithm: find the kth-smallest element with median-of-median pivoting.
 * @author yexijiang
 *
 */
public class BFPRT {
	
	/**
	 * Find the ith-smallest elements in tupleList in O(n).
	 * @param tupleList
	 * @param i
	 * @return
	 */
	public static Tuple bfprt(List<Tuple> tupleList, int i) {
		
		List<TupleWrapper> tupleWrapperList = new ArrayList<TupleWrapper>();
		for(Tuple tuple : tupleList) {
			tupleWrapperList.add(new TupleWrapper(tuple.getLong(1), tuple));
		}
		
		return bfprtWrapper(tupleWrapperList, i, 0, tupleWrapperList.size()).tuple;
	}
	
	public static TupleWrapper bfprtWrapper(List<TupleWrapper> tupleWrapperList, int i, int left, int right) {
		
		if(left == right) {
			return tupleWrapperList.get(left);
		}
		
		int q = partitionSingleSide(tupleWrapperList, left, right);
		
		int k = q - left + 1; 
		if(i > k) {	//	recursively find right part
			return bfprtWrapper(tupleWrapperList, q + 1, right, i);
		}
		else if (i < k) {	//	recursively find left part
			return bfprtWrapper(tupleWrapperList, left, q - 1, i - k);
		}
		else {
			return tupleWrapperList.get(q);
		}
	}
	
	/**
	 * Partition single side version.
	 * @param tupleWrapperList
	 * @param left
	 * @param right
	 * @return
	 */
	public static int partitionSingleSide(List<TupleWrapper> tupleWrapperList, int left, int right) {
		int pivotIdx = right;
		TupleWrapper pivot = tupleWrapperList.get(pivotIdx);
		int bar = left - 1;
		
		for(int i = left; i < right; ++i) {
			if(tupleWrapperList.get(i).compareTo(pivot) < 0) {
				++bar;
				swap(tupleWrapperList, bar, i);
			}
		}
		swap(tupleWrapperList, bar + 1, pivotIdx);
		return bar + 1;
	}
	
	
	/**
	 * Sort the group-of-5 with insertionSort.
	 * @param tupleList
	 * @param left
	 * @param right
	 */
	public static void insertionSort(List<TupleWrapper> tupleWrapperList, int left, int right) {
		for(int i = left + 1; i < right; ++i) {
			int iHole = i;
			TupleWrapper wrapper = tupleWrapperList.get(iHole);
			while(iHole > 0 && tupleWrapperList.get(iHole - 1).compareTo(wrapper) > 0) {
				tupleWrapperList.set(iHole - 1, wrapper);
				--iHole;
			}
			tupleWrapperList.set(iHole, wrapper);
		}
	}
	
	private static void swap(List<TupleWrapper> tupleWrapperList, int left, int right) {
		TupleWrapper tmp = tupleWrapperList.get(left);
		tupleWrapperList.set(left, tupleWrapperList.get(right));
		tupleWrapperList.set(right, tmp);
	}
	
	/**
	 * The wrapper that make the BFPRT algorithm more generic.
	 * @author yexijiang
	 *
	 */
	public static class TupleWrapper implements Comparable {
		
		double score;
		Tuple tuple;
		
		public TupleWrapper(double score, Tuple tuple) {
			super();
			this.score = score;
			this.tuple = tuple;
		}

		@Override
		public int compareTo(Object o) {
			if(o instanceof TupleWrapper) {
				TupleWrapper wrapper = (TupleWrapper)o;
				if(score == wrapper.score) {
					return 0;
				}
				else if(score > wrapper.score) { 
					return 1;
				}
				else {
					return -1;
				}
			}
			return 0;
		}
		
	}
	
}
