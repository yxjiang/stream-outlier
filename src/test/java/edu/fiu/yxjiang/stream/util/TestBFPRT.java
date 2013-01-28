package edu.fiu.yxjiang.stream.util;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import edu.fiu.yxjiang.stream.util.BFPRT.TupleWrapper;

public class TestBFPRT {
	
	private List<TupleWrapper> wrapperList;
	
	@Before
	public void before() {
		Random rnd = new Random();
		wrapperList = new ArrayList<TupleWrapper>();
		System.out.println("Before");
		for(int i = 0; i < 2; ++i) {
			TupleWrapper wrapper = new TupleWrapper(rnd.nextInt(500), null);
			wrapperList.add(wrapper);
			System.out.print(wrapper.score + "\t");
		}
		System.out.println();
//		wrapperList.add(new TupleWrapper(2111.0, null));
//		wrapperList.add(new TupleWrapper(33.0, null));
//		wrapperList.add(new TupleWrapper(39.0, null));
//		wrapperList.add(new TupleWrapper(15.0, null));
//		wrapperList.add(new TupleWrapper(11.0, null));
//		wrapperList.add(new TupleWrapper(11.0, null));
//		wrapperList.add(new TupleWrapper(5.0, null));
	}
	
	@Ignore
	@Test
	public void testInsertionSort() {
		BFPRT.insertionSort(wrapperList, 0, wrapperList.size());
		int previous = 0;
		for(TupleWrapper wrapper : wrapperList) {
			assertTrue(previous - wrapper.score <= 0);
		}
	}
	
	@Ignore
	@Test
	public void testPartition() {
		int k = BFPRT.partitionSingleSide(wrapperList, 0, wrapperList.size() - 1);
		TupleWrapper pivotWrapper = wrapperList.get(k);
		
		for(int i = 0; i < wrapperList.size(); ++i) {
			if(i == k) {
				System.out.print("[");
			}
			System.out.print(wrapperList.get(i).score);
			if(i == k) {
				System.out.print("]");
			}
			System.out.print("\t");
		}
		System.out.println();
		
		for(int i = 0; i < k; ++i) {
			assertTrue(pivotWrapper.compareTo(wrapperList.get(i)) >= 0);
		}
		for(int i = k + 1; i < wrapperList.size(); ++i) {
			assertTrue(pivotWrapper.compareTo(wrapperList.get(i)) <= 0);
		}
	}
	
//	@Ignore
	@Test
	public void testBfprtWrapper() {
		int k = (int)Math.round(wrapperList.size() / 2);
		System.out.println(k);
//		BFPRT.insertionSort(wrapperList, 0, wrapperList.size() - 1);
//		TupleWrapper median = wrapperList.get(k);
	
		TupleWrapper median = BFPRT.bfprtWrapper(wrapperList, k, 0, wrapperList.size() - 1);
		
		for(int i = 0; i < wrapperList.size(); ++i) {
			System.out.print(wrapperList.get(i).score + "\t");
		}
		System.out.println();
		
		System.out.println("Median:" + median.score);
	}
	
}
