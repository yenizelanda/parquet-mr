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
import parquet.column.values.delta.DeltaBitPackingValuesWriter.MODE;

public class TestDeltaBitPacking {

	@Test
	public void TestCloseIntValues() throws IOException {

		int[] ints = { 1000, 998, 996, 990, 995, 1002, 1004, 999, 997 };

		control(ints);

	}

	@Test
	public void TestRandomCloseIntValues() throws IOException {
		int[] ints = new int[9645];

		int range = 5;

		ints[0] = 2483265;

		for (int i = 1; i < ints.length; i++)
			ints[i] = ints[i - 1]
					+ (((int) (Math.random() * (2 * range + 1))) - range);

		control(ints);
	}

	@Test
	public void TestLargeVariations() throws IOException {
		int[] ints = new int[30];

		for (int i = 0; i < ints.length; i++) {
			if (i % 2 == 0)
				ints[i] = Integer.MAX_VALUE - ((int) (1000 * Math.random()));
			else
				ints[i] = ((int) (1000 * Math.random()));
		}

		control(ints);
	}

	private void control(int[] ints) throws IOException {

		MODE mode = MODE.PACK_8;

		DeltaBitPackingValuesWriter dvw = new DeltaBitPackingValuesWriter(50,
				mode);

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
