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

import java.io.IOException;

import parquet.bytes.BytesUtils;
import parquet.column.values.ValuesReader;
import parquet.io.ParquetDecodingException;

public class DeltaValuesReader extends ValuesReader {

	private boolean isFirst;
	private int lastNumber;
	private int nextNumber;
	private byte[] b;
	private int currentOffset;

	public DeltaValuesReader() {
		isFirst = true;
	}

	@Override
	public int initFromPage(long valueCount, byte[] page, int offset)
			throws IOException {
		b = page;
		currentOffset = offset;
		isFirst = true;
		return offset + 4 * ((int) valueCount);
	}

	@Override
	public int readInteger() {

		if (isFirst) {
			try {
				nextNumber = BytesUtils.readIntLittleEndian(b, currentOffset);
				currentOffset += 4;
			} catch (IOException e) {
				throw new ParquetDecodingException("could not read int", e);
			}
			lastNumber = nextNumber;
			isFirst = false;
			return nextNumber;
		} else {
			try {
				nextNumber = lastNumber
						+ zigzagDecode(BytesUtils.readIntLittleEndian(b,
								currentOffset));
				currentOffset += 4;
			} catch (IOException e) {
				throw new ParquetDecodingException("could not read int", e);
			}
			lastNumber = nextNumber;
			return nextNumber;
		}

	}

	private int zigzagDecode(int v) {
		if (v % 2 == 1) {
			return v / 2 + 1;
		} else {
			return -v / 2;
		}
	}

}
