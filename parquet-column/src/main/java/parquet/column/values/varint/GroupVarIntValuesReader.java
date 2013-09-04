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

import java.io.ByteArrayInputStream;
import java.io.IOException;

import parquet.bytes.BytesUtils;
import parquet.column.values.ValuesReader;

/**
 * This class decodes the page using group varint method.
 * @author Baris Kaya
 *
 */

public class GroupVarIntValuesReader extends ValuesReader{

	/**
	 * The first byte of a group of 4 integers to be read.
	 */
	private int firstByte;
	
	/**
	 * The place of the next integer amongst the 4 integers.
	 */
	private int readOffset;
	
	/**
	 * The input stream to put the input to, and later on read from.
	 */
	private ByteArrayInputStream in;
	
	public GroupVarIntValuesReader() {
	}
	
	public int initFromPage(long valueCount, byte[] page, int offset)
			throws IOException {
		
		//initialize variables
		readOffset = 0;
	    in = new ByteArrayInputStream(page, offset, page.length - offset);
		//quickly go over the integers to figure out where the page ends
	    for (int i = 0; i < valueCount; i += 4, offset += 1 + VarIntHelper.totalLength[(page[offset] + 256) % 256]);
		//read the first byte
	    firstByte = in.read();
		return offset;
		
	}

	public int readInteger() {

		int retVal = 0;
		try {
			//get the next number with a number of bytes from the input stream. the number of bytes to read is encoded in firstbyte.
			retVal = VarIntHelper.zigzagDecode(BytesUtils.readIntLittleEndianPaddedOnBitWidth(in, 8 * VarIntHelper.length[firstByte][readOffset++]));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		//reset readOffset, and read a new firstByte
		if (readOffset == 4) {
			readOffset = 0;
			firstByte = in.read();
		}
		
		return retVal;
		
		
	}

	public void skip() {
		//skip the necessary amount of bytes from the input, and reset the firstByte if necessary
		in.skip(VarIntHelper.length[firstByte][readOffset++]);
		if (readOffset == 4) {
			readOffset = 0;
			firstByte = in.read();
		}
	}
}