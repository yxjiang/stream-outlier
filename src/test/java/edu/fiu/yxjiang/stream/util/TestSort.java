package edu.fiu.yxjiang.stream.util;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import org.junit.Test;

public class TestSort {

	@Test
	public void testSort() {
		class Wrapper {
			public int elem;
			public Wrapper(int elem) {
				this.elem = elem;
			}
			public String toString() {
				return "" + elem;
			}
		}
		
		for (int attempts = 0; attempts < 100; ++ attempts) {
			List<Wrapper> list = new ArrayList<Wrapper>();
			Random rnd = new Random();
			int size = rnd.nextInt(100000);
			System.out.println("Size:" + size);
			
			for (int i = 0; i < size; ++i) {
				list.add(new Wrapper(rnd.nextInt(1000)));
			}
			Sorter.quicksort(list, new Comparator<Wrapper>() {
	
				@Override
				public int compare(Wrapper o1, Wrapper o2) {
					if (o1.elem < o2.elem) {
						return -1;
					}
					else if (o1.elem == o2.elem) {
						return 0;
					}
					else {
						return 1;
					}
				}
				
			});
			
			for (int i = 1; i < size; ++i) {
				assertTrue(list.get(i - 1).elem <= list.get(i).elem);
			}
		}
	}
}
