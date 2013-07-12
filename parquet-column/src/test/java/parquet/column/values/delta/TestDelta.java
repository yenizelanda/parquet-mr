/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.column.values.delta;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

import parquet.bytes.BytesInput;

public class TestDelta {

	@Test
	public void TestCloseIntValues() throws IOException {
		
		int[] ints = {1000, 998, 996, 990, 995, 1002, 1004, 999, 997};
		
		DeltaValuesWriter dvw = new DeltaValuesWriter(40);
		
		for (int i : ints) {
			dvw.writeInteger(i);
		}
		
		BytesInput bi = dvw.getBytes();
		
		DeltaValuesReader dvr = new DeltaValuesReader();
		
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
