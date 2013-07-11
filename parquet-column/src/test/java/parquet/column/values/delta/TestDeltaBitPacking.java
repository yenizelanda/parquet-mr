package parquet.column.values.delta;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

import parquet.bytes.BytesInput;

public class TestDeltaBitPacking {

	@Test
	public void TestCloseIntValues() throws IOException {
		
		int[] ints = {1000, 998, 996, 990, 995, 1002, 1004, 999, 997};

		
		control(ints);
		
	}
	
	@Test
	public void TestRandomCloseIntValues() throws IOException {
		int[] ints = new int[9645];
		
		int range = 5;
		
		ints[0] = 2483265;
		
		for (int i = 1; i < ints.length; i++)
			ints[i] = ints[i-1] + (((int) (Math.random()*(2*range+1)))-range);
		
		control(ints);
	}
	
	private void control(int[] ints) throws IOException {
		
		DeltaBitPackingValuesWriter dvw = new DeltaBitPackingValuesWriter(50);
		
		for (int i : ints) {
			dvw.writeInteger(i);
		}
		
		BytesInput bi = dvw.getBytes();

		DeltaBitPackingValuesReader dvr = new DeltaBitPackingValuesReader();
		
		dvr.initFromPage(ints.length, bi.toByteArray(), 0);
		
	    String expected = "";
	    String got = "";
	    for (int i : ints) {
	      expected += " " + i;
	      got += " " + dvr.readInteger();
	    }
	    assertEquals(expected, got);
		
	}
}
