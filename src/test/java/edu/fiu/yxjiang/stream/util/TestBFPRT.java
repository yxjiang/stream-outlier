package edu.fiu.yxjiang.stream.util;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import edu.fiu.yxjiang.stream.util.BFPRT.TupleWrapper;

public class TestBFPRT {
	
	private List<TupleWrapper> wrapperList;
	
	@Before
	public void before() {
		Random rnd = new Random();
		wrapperList = new ArrayList<TupleWrapper>();
		for(int i = 0; i < 18; ++i) {
			TupleWrapper wrapper = new TupleWrapper(null, rnd.nextInt(5000));
			wrapperList.add(wrapper);
		}
	}
	
	@Test
	public void testInsertionSort() {
		BFPRT.insertionSort(wrapperList, 0, wrapperList.size());
		int previous = 0;
		for(TupleWrapper wrapper : wrapperList) {
			assertTrue(previous - wrapper.score <= 0);
		}
	}
}
