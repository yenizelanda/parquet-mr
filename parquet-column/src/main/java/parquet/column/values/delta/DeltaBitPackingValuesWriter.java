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
import parquet.Log;
import parquet.bytes.BytesInput;
import parquet.bytes.CapacityByteArrayOutputStream;
import parquet.column.Encoding;
import parquet.column.values.ValuesWriter;
import parquet.column.values.bitpacking.ByteBitPackingLE;
import parquet.column.values.bitpacking.BytePacker;

/**
 * This class uses delta encoding combined with bit packing to
 * increase the performance of the encoding process.
 * 
 * @author Baris Kaya
 *
 */
public class DeltaBitPackingValuesWriter extends ValuesWriter {

	private static final Log LOG = Log.getLog(DeltaBitPackingValuesWriter.class);
	
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
	 * Output Stream which stores the encoded data and turns it into a 
	 * byte array at the end.
	 */
	private final CapacityByteArrayOutputStream cbaos;
	
	/**
	 * The buffer of currently un-encoded values.
	 */
	private final int[] buffer;
	
	/**
	 * The position to write into the buffer.
	 */
	private int bufferOffset;
	
	/**
	 * The running tally of the values currently stored in buffer.
	 */
	private int bitTally;
	
	/**
	 * The last number that was read. This value is necessary to delta encode
	 * the integers.
	 */
	private int lastNumber;
	
	/**
	 * True if the currently read integer is the first one.
	 */
	private boolean isFirst;
	
	/**
	 * The BytePacker which encodes the buffer before it is stored to baos.
	 */
	private BytePacker packer;
	
	/**
	 * temporary buffer where the encoded values are stored before they are 
	 * written onto baos.
	 */
	private final byte[] temp;
	
	/**
	 * Holds the current mode of the writer.
	 */
	private MODE mode;
	
	/**
	 * The enum for modes, helps to tell the writer if values are packed 8 at a 
	 * time or 32 at a time.
	 * @author Baris Kaya
	 *
	 */
	public enum MODE {

		/**
		 * Values are being packed 8 at a time.
		 */
		PACK_8,
		/**
		 * Values are being packed 32 at a time.
		 */
		PACK_32
	}

	public DeltaBitPackingValuesWriter(int initialCapacity, MODE mode) {
		cbaos = new CapacityByteArrayOutputStream(initialCapacity);
		isFirst = true;
		this.mode = mode;

		if (mode == MODE.PACK_32) {
			cbaos.write(1);
			// buffer up to BUFFER_SIZE_PACK_32 integers
			buffer = new int[BUFFER_SIZE_PACK_32];

			// make sure temp has enough slots to store the temporary encoded values
			temp = new byte[Integer.SIZE * 4];
		    if (DEBUG) LOG.debug("initializing the writer with PACK_32 mode");
		}
		else {
			cbaos.write(0);
			// buffer up to BUFFER_SIZE_PACK_8 integers
			buffer = new int[BUFFER_SIZE_PACK_8];

			// make sure temp has enough slots to store the temporary encoded values
			temp = new byte[Integer.SIZE];
		    if (DEBUG) LOG.debug("initializing the writer with PACK_8 mode");
		}
		
		// establish buffer size
		bufferSize = buffer.length;
		
		// current position within the buffer
		bufferOffset = 0;

		// running tally of the bits in the buffer
		bitTally = 0;
	}

	private void flushBuffer() {
		// determine how many bits are necessary to encode
		int maxBits = Integer.SIZE - Integer.numberOfLeadingZeros(bitTally);
		

		// record down the max bit
		cbaos.write((byte) maxBits);

		// lookup the appropriate packer
		packer = ByteBitPackingLE.getPacker(maxBits);

		if (mode == MODE.PACK_32) {
		    if (DEBUG) LOG.debug("packing 32 values at once");
			// pack the buffer into the temp byte array, 32 at a time
			packer.pack32Values(buffer, 0, temp, 0);
	
			// write out the compressed data
			// the packer uses 32 * maxBits bits to store the data, and that
			// corresponds to 4 * maxBits bytes
			cbaos.write(temp, 0, 4 * maxBits);
		}
		else {
		    if (DEBUG) LOG.debug("packing 8 values at once");
			// pack the buffer into the temp byte array, 8 at a time
			packer.pack8Values(buffer, 0, temp, 0);
	
			// write out the compressed data
			// the packer uses 8 * maxBits bits to store the data, and that
			// corresponds to maxBits bytes
			cbaos.write(temp, 0, maxBits);
			
		}

		// reset the offset and the bit tally
		bufferOffset = 0;
		bitTally = 0;
	}

	@Override
	public long getAllocatedSize() {
		return cbaos.getCapacity();
	}

	@Override
	public long getBufferedSize() {
		if (bufferOffset == 0) {
			// nothing in the current buffer
			return cbaos.size();
		} else {
			// try to account for the buffered data, one byte is used to store
			// the bit size
			int maxBits = (byte) (Integer.SIZE - Integer.numberOfLeadingZeros(bitTally));
			int packedSize = 4 * maxBits;
			return cbaos.size() + 1 + packedSize;
		}
	}

	@Override
	public BytesInput getBytes() {
		if (bufferOffset != 0) {
			// flush the buffer unless it is empty, and even
			// if it isn't full yet
			flushBuffer();
		}
		
		//turn the encoded data into a BytesInput object
		return BytesInput.from(cbaos);
	}

	@Override
	public Encoding getEncoding() {
		return Encoding.DELTA;
	}

	@Override
	public String memUsageString(String prefix) {
		return cbaos.memUsageString(prefix);
	}

	@Override
	public void reset() {
		// reset the variables
		cbaos.reset();
		isFirst = true;
		bufferOffset = 0;
		bitTally = 0;
	}

	@Override
	public void writeInteger(int v) {
		if (isFirst) {
			// this is the first number, so store it without encoding it
			isFirst = false;
			buffer[bufferOffset++] = v;
		    if (DEBUG) LOG.debug("writing an integer to the buffer");
		} else {
			// compute the delta, and zigzag encode it
			int delta = DeltaEncoding.zigzagEncode(v - lastNumber);

			// store and increment offset
			buffer[bufferOffset++] = delta;
		    if (DEBUG) LOG.debug("writing an integer to the buffer");

		}
		// update the bit tally
		bitTally = bitTally | buffer[bufferOffset-1];

		if (bufferOffset == bufferSize) {
			// buffer is full, flush
			flushBuffer();
		}
		lastNumber = v;
	}

}
