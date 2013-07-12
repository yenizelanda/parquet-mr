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
import parquet.column.values.ValuesReader;
import parquet.column.values.bitpacking.ByteBitPackingLE;
import parquet.column.values.bitpacking.BytePacker;
import parquet.column.values.delta.DeltaBitPackingValuesWriter.MODE;

/**
 * This class uses delta encoding combined with bit packing to increase the
 * performance of the encoding process.
 * 
 * @author Baris Kaya
 * 
 */
public class DeltaBitPackingValuesReader extends ValuesReader {

	private static final Log LOG = Log
			.getLog(DeltaBitPackingValuesReader.class);

	/**
	 * Buffer size if we choose to pack 32 integers at a time.
	 */
	private static final int BUFFER_SIZE_PACK_32 = 32;

	/**
	 * Buffer size if we choose to pack 8 integers at a time.
	 */
	private static final int BUFFER_SIZE_PACK_8 = 8;

	/**
	 * Variable to store the size of the buffer.
	 */
	private static int bufferSize;

	/**
	 * True if the currently read integer is the first one.
	 */
	private boolean isFirst;

	/**
	 * The last number that was read. This value is necessary to delta encode
	 * the integers.
	 */
	private int lastNumber;

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

	public DeltaBitPackingValuesReader() {

	}

	private void flushToBuffer() {
		// read bit width from the page.
		byte maxBits = currentPage[currentOffset++];

		// get the corresponding packer
		packer = ByteBitPackingLE.getPacker(maxBits);

		if (mode == MODE.PACK_32) {
			if (DEBUG)
				LOG.debug("unpacking 32 values at once");
			// decode 32 values at once, writing it on buffer.
			packer.unpack32Values(currentPage, currentOffset, buffer, 0);

			currentOffset += 4 * maxBits;
		} else {
			if (DEBUG)
				LOG.debug("unpacking 8 values at once");
			// decode 8 values at once, writing it on buffer.
			packer.unpack8Values(currentPage, currentOffset, buffer, 0);

			currentOffset += maxBits;
		}

		bufferOffset = 0;
	}

	@Override
	public int initFromPage(long valueCount, byte[] page, int offset)
			throws IOException {
		if (page[offset++] == 1)
			mode = MODE.PACK_32;
		else
			mode = MODE.PACK_8;
		// change buffer size depending on the pack mode of the writer.
		if (mode == MODE.PACK_32) {
			buffer = new int[BUFFER_SIZE_PACK_32];
			if (DEBUG)
				LOG.debug("initializing the reader with PACK_32 mode");
		} else {
			buffer = new int[BUFFER_SIZE_PACK_8];
			if (DEBUG)
				LOG.debug("initializing the reader with PACK_8 mode");
		}
		bufferSize = buffer.length;
		// initialize the variables.
		currentPage = page;
		currentOffset = offset;
		isFirst = true;
		bufferOffset = bufferSize;

		// skim over the page to find out where it ends. To do this, this loop
		// quickly reads over the bit widths, and jumps one block at a time
		while (valueCount > 0) {
			if (mode == MODE.PACK_32)
				// this block consists of 1 bitWidth and 4 * bitWidth encoded
				// values
				offset += 1 + 4 * page[offset];
			else
				// this block consists of 1 bitWidth and bitWidth encoded values
				offset += 1 + page[offset];

			// decrement to know how many values are remaining
			valueCount -= bufferSize;
			;
		}

		return offset;
	}

	@Override
	public int readInteger() {

		if (bufferOffset == bufferSize) {
			// we finished reading from the buffer, so fill the buffer again
			flushToBuffer();
		}

		if (isFirst) {
			// this is the first number, so the integer read from the buffer
			// doesn't need
			// encoding
			nextNumber = buffer[bufferOffset++];
			if (DEBUG)
				LOG.debug("reading an integer and writing it to buffer");
			lastNumber = nextNumber;
			isFirst = false;
			return nextNumber;
		} else {
			// read from the buffer and decode it to obtain a next number
			nextNumber = lastNumber
					+ DeltaEncoding.zigzagDecode(buffer[bufferOffset++]);
			if (DEBUG)
				LOG.debug("reading an integer, delta encoding it and writing it to buffer");
			lastNumber = nextNumber;
			return nextNumber;
		}
	}

}
