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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import parquet.Log;
import parquet.bytes.BytesUtils;
import parquet.column.values.bitpacking.ByteBitPackingLE;
import parquet.column.values.bitpacking.BytePacker;
import parquet.io.ParquetDecodingException;

/**
 * Decodes values written in the grammar described in
 * {@link RunLengthBitPackingHybridEncoder}
 * 
 * @author Julien Le Dem
 */
public class VariableWidthRunLengthBitPackingHybridDecoder {
	private static final Log LOG = Log
			.getLog(VariableWidthRunLengthBitPackingHybridDecoder.class);

	private static enum MODE {
		RLE, PACKED
	}

	private int packerWidth;
	
	private BytePacker packer;
	private final InputStream in;
	private byte[] bytes;

	private MODE mode;

	private int currentCount;
	private int currentValue;
	private int[] currentBuffer;

	public VariableWidthRunLengthBitPackingHybridDecoder(InputStream in) {
		if (DEBUG)
			LOG.debug("decoding bitWidth " + 32);

		this.in = in;
		bytes  = new byte[32];
	}

	public int readInt() throws IOException {
		if (currentCount == 0) {
			readNext();
		}
		--currentCount;
		int result;
		switch (mode) {
		case RLE:
			result = currentValue;
			break;
		case PACKED:
			result = currentBuffer[currentBuffer.length - 1 - currentCount];
			break;
		default:
			throw new ParquetDecodingException("not a valid mode " + mode);
		}
		return result;
	}

	private void readNext() throws IOException {
		final int header = BytesUtils.readUnsignedVarInt(in);
		mode = (header & 1) == 0 ? MODE.RLE : MODE.PACKED;
		switch (mode) {
		case RLE:
			currentCount = header >>> 1;
			if (DEBUG)
				LOG.debug("reading " + currentCount + " values RLE");
			currentValue = BytesUtils.readIntLittleEndian(in);
			break;
		case PACKED:
			int numGroups = header >>> 1;
			currentCount = numGroups * 8;
			if (DEBUG)
				LOG.debug("reading " + currentCount + " values BIT PACKED");
			currentBuffer = new int[currentCount]; // TODO: reuse a buffer
			
			for (int valueIndex = 0; valueIndex < currentCount; valueIndex += 8) {
				packerWidth = new DataInputStream(in).read();
				new DataInputStream(in).read(bytes, 0, packerWidth);
				packer = ByteBitPackingLE.getPacker(packerWidth);
				packer.unpack8Values(bytes, 0, currentBuffer,
						valueIndex);
			}
			break;
		default:
			throw new ParquetDecodingException("not a valid mode " + mode);
		}
	}
}
