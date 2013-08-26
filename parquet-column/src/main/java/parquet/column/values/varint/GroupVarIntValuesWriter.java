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
package parquet.column.values.varint;

import java.io.IOException;

import parquet.bytes.BytesInput;
import parquet.bytes.BytesUtils;
import parquet.bytes.CapacityByteArrayOutputStream;
import parquet.column.Encoding;
import parquet.column.values.ValuesWriter;

/**
 * This class first encodes numbers with zigzag encoding to make them positive,
 * then uses group varint for a variable length integer encoding.
 * @author Baris Kaya
 */
public class GroupVarIntValuesWriter extends ValuesWriter {

	public final CapacityByteArrayOutputStream cbaos;
	
	private int[] buffer;
	private int bufferOffset;

	
	public GroupVarIntValuesWriter(int initialCapacity) {
		cbaos = new CapacityByteArrayOutputStream(initialCapacity);
		buffer = new int[4];
		bufferOffset = 0;
		
	}
	
	public long getBufferedSize() {
		return cbaos.size();
	}

	public BytesInput getBytes() {
		if (bufferOffset != 0) {
			for (int i = bufferOffset; i < 4; i++)
				buffer[i] = 0;
			writeToOutputStream();
		}
		return BytesInput.from(cbaos);
		
	}

	public Encoding getEncoding() {
		return Encoding.GROUP_VAR_INT;
	}

	public void reset() {
		cbaos.reset();
		bufferOffset = 0;
	}

	public long getAllocatedSize() {
		return cbaos.getCapacity();
	}

	public void writeInteger(int v) {
		buffer[bufferOffset++] = VarIntHelper.encode(v);
		
		if (bufferOffset == 4)
			writeToOutputStream();
	}

	private void writeToOutputStream() {
		
		int firstByte = 0;
		for (int i = 0; i < 4; i++) {
			firstByte = firstByte << 2;
			firstByte += Math.max(0,3 - Integer.numberOfLeadingZeros(buffer[i])/8);
		}
		
		cbaos.write(firstByte);
		
		for (int i = 0; i < 4; i++) {
			try {
				BytesUtils.writeIntLittleEndianPaddedOnBitWidth(cbaos, buffer[i], 8 * VarIntHelper.length[firstByte][i]);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		bufferOffset = 0;		
	}

	public String memUsageString(String prefix) {
		return cbaos.memUsageString(prefix);
	}

}
