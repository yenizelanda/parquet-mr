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

import static parquet.Log.DEBUG;

import java.io.IOException;

import parquet.Log;
import parquet.bytes.BytesUtils;
import parquet.column.values.ValuesReader;
import parquet.column.values.bitpacking.ByteBitPackingLE;
import parquet.column.values.bitpacking.BytePacker;
import parquet.column.values.delta.DiffBitPackingValuesWriter.MODE;

/**
 * This class uses delta encoding combined with bit packing to increase the
 * performance of the encoding process.
 * 
 * @author Baris Kaya
 * 
 */
public class DiffBitPackingValuesReader extends ValuesReader {

	private static final Log LOG = Log
			.getLog(DiffBitPackingValuesReader.class);

	/**
	 * Variable to store the size of the buffer.
	 */
	private static int bufferSize;

	
	private int currentMin;


	/**
	 * next number in the original array.
	 */
	private int nextNumber;

	/**
	 * The page where the encoded data is read from
	 */
	private byte[] currentPage;

	/**
	 * The current position of the data on the currentPage.
	 */
	private int currentOffset;

	/**
	 * The buffer of currently un-encoded values.
	 */
	private int[] buffer;

	/**
	 * The position to write into the buffer.
	 */
	private int bufferOffset;

	/**
	 * The BytePacker which encodes the buffer before it is stored to baos.
	 */
	private BytePacker packer;

	/**
	 * Holds the current mode of the writer.
	 */
	private MODE mode;
	
	/**
	 * The enum for modes, helps to tell the writer if values are packed 8 at a
	 * time or 32 at a time.
	 * 
	 * @author Baris Kaya
	 * 
	 */

	public DiffBitPackingValuesReader() {

	}

	private void flushToBuffer() {
		// read bit width from the page.
		byte maxBits = currentPage[currentOffset++];

		// get the corresponding packer
		packer = ByteBitPackingLE.factory.newBytePacker(maxBits);

		if (mode == MODE.PACK_32) {
			if (DEBUG)
				LOG.debug("unpacking 32 values at once");
			// decode 32 values at once, writing it on buffer.
			packer.unpack32Values(currentPage, currentOffset, buffer, 0);
		} else {
			if (DEBUG)
				LOG.debug("unpacking 8 values at once");
			// decode 8 values at once, writing it on buffer.
			packer.unpack8Values(currentPage, currentOffset, buffer, 0);
		}

		currentOffset += mode.getOutputSizeMultiplier() * maxBits;

		bufferOffset = 0;
	}

	@Override
	public int initFromPage(long valueCount, byte[] page, int offset)
			throws IOException {
		if (page[offset++] == 1) {
			mode = MODE.PACK_32;
			if (DEBUG)
				LOG.debug("initializing the reader with PACK_32 mode");
		} else {
			mode = MODE.PACK_8;
			if (DEBUG)
				LOG.debug("initializing the reader with PACK_8 mode");
		}
		// change buffer size depending on the pack mode of the writer.
		buffer = new int[mode.getBufferSize()];

		// initialize the variables.
		bufferSize = mode.getBufferSize();
		currentPage = page;
		bufferOffset = bufferSize;

		currentMin = BytesUtils.readIntLittleEndian(page, offset);
		offset += 4;
		currentOffset = offset;
		
		// skim over the page to find out where it ends. To do this, this loop
		// quickly reads over the bit widths, and jumps one block at a time
		while (valueCount > 0) {
			offset += 1 + mode.getOutputSizeMultiplier() * page[offset];
			valueCount -= bufferSize;
		}

		return offset;
	}

	@Override
	public int readInteger() {

		if (bufferOffset == bufferSize) {
			// we finished reading from the buffer, so fill the buffer again
			flushToBuffer();
		}

	
		// read from the buffer and decode it to obtain a next number
		nextNumber = buffer[bufferOffset++] + currentMin;
		if (DEBUG)
			LOG.debug("reading an integer, delta encoding it and writing it to buffer");
		return nextNumber;
		
	}

	@Override
	/**
	 * There is no faster way to skip the next value. 
	 * The next value must be read and lastInteger has to be updated for future readInteger calls.
	 */
	public void skip() {

		readInteger();
	}

}
