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
import parquet.bytes.BytesInput;
import parquet.bytes.BytesUtils;
import parquet.bytes.CapacityByteArrayOutputStream;
import parquet.column.Encoding;
import parquet.column.values.ValuesWriter;
import parquet.column.values.bitpacking.ByteBitPackingLE;
import parquet.column.values.bitpacking.BytePacker;

/**
 * This class uses difference based encoding combined with bit packing to increase the
 * performance of the encoding process.
 * 
 * @author Baris Kaya
 * 
 */
public class DiffBitPackingValuesWriter extends ValuesWriter {

	private static final Log LOG = Log
			.getLog(DiffBitPackingValuesWriter.class);

	/**
	 * Output Stream which stores the encoded data and turns it into a byte
	 * array at the end.
	 */
	private final CapacityByteArrayOutputStream cbaos;

	/**
	 * The buffer of currently un-encoded values.
	 */
	private final int[] buffer;
	
	/**
	 * The buffer which stores every value inserted, before writing it to baos.
	 */
	private final int[] storage;
	
	
	/**
	 * The position to write into to the storage array. 
	 */
	private int storageOffset;
	
	/**
	 * The current minimum so far.
	 */
	private int currentMin;
	
	/**
	 * Number of bits necessary for the current package.
	 */
	private int numBits;
	
	/**
	 * The estimated compression size. Having an actual size is too time consuming,
	 * so there is only an estimate.
	 */
	private int compressedSize;

	/**
	 * The position to write into the buffer.
	 */
	private int bufferOffset;


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
	 * 
	 * @author Baris Kaya
	 * 
	 */
	public enum MODE {

		/**
		 * Values are being packed 8 at a time.
		 */
		PACK_8 {
			public int getOutputSizeMultiplier() {
				return 1;
			}

			public int getBufferSize() {
				return 8;
			}
		},
		/**
		 * Values are being packed 32 at a time.
		 */
		PACK_32 {
			public int getOutputSizeMultiplier() {
				return 4;
			}

			public int getBufferSize() {
				return 32;
			}
		};

		public abstract int getOutputSizeMultiplier();

		public abstract int getBufferSize();
	}

	public DiffBitPackingValuesWriter(int initialCapacity, MODE mode) {
		cbaos = new CapacityByteArrayOutputStream(initialCapacity);
		this.mode = mode;

		if (mode == MODE.PACK_32) {
			cbaos.write(1);
			if (DEBUG)
				LOG.debug("initializing the writer with PACK_32 mode");
		} else {
			cbaos.write(0);
			if (DEBUG)
				LOG.debug("initializing the writer with PACK_8 mode");
		}

		storage = new int[300000];
		
		currentMin = Integer.MAX_VALUE;
		
		storageOffset = 0;
		
		numBits = 0;
		
		// create a buffer big enough to store values until they get packed
		buffer = new int[mode.getBufferSize()];

		// make sure temp has enough slots to store the temporary encoded
		// values
		temp = new byte[Integer.SIZE * mode.getOutputSizeMultiplier()];

		compressedSize = 1;

		// current position within the buffer
		bufferOffset = 0;

	}

	@Override
	public long getAllocatedSize() {
		return cbaos.getCapacity();
	}

	@Override
	public long getBufferedSize() {
		return compressedSize + 1 + numBits * mode.getOutputSizeMultiplier();
	}

	@Override
	public BytesInput getBytes() {
		
		try {
			BytesUtils.writeIntLittleEndian(cbaos, currentMin);
		} catch (IOException e) {
			System.out.println("Failed to write to output stream: " + e.getMessage());
		}
		
		for (int i = 0; i < storageOffset; i+=mode.getBufferSize()) {
			numBits = 0;
			for (bufferOffset = 0; bufferOffset < mode.getBufferSize() && i + bufferOffset < storageOffset; bufferOffset++) {
				numBits = Math.max(numBits, 32 - Integer.numberOfLeadingZeros(storage[bufferOffset + i] - currentMin));
				buffer[bufferOffset] = storage[bufferOffset + i] - currentMin;
			}

			// record down the max bit
			cbaos.write((byte) numBits);

			// lookup the appropriate packer
			packer = ByteBitPackingLE.factory.newBytePacker(numBits);

			if (mode == MODE.PACK_32) {
				if (DEBUG)
					LOG.debug("packing 32 values at once");
				// pack the buffer into the temp byte array, 32 at a time
				packer.pack32Values(buffer, 0, temp, 0);
			} else {
				if (DEBUG)
					LOG.debug("packing 8 values at once");
				// pack the buffer into the temp byte array, 8 at a time
				packer.pack8Values(buffer, 0, temp, 0);
			}
			cbaos.write(temp, 0, mode.getOutputSizeMultiplier() * numBits);
		}
		return BytesInput.from(cbaos);
	}

	@Override
	public Encoding getEncoding() {
		return Encoding.DIFF;
	}

	@Override
	public String memUsageString(String prefix) {
		return cbaos.memUsageString(prefix);
	}

	@Override
	public void reset() {
		// reset the variables
		cbaos.reset();
		bufferOffset = 0;
	}

	@Override
	public void writeInteger(int v) {

		currentMin = Math.min(currentMin, v);
		
		storage[storageOffset++] = v;
		
		numBits = Math.max(numBits, 32 - Integer.numberOfLeadingZeros(v - currentMin));
		
		if (storageOffset % mode.getBufferSize() == 0) {
			compressedSize += 1 + numBits * mode.getOutputSizeMultiplier();
			numBits = 0;
		}
	}

}
