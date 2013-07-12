package parquet.column.values.delta;

import parquet.bytes.BytesInput;
import parquet.bytes.BytesUtils;
import parquet.bytes.CapacityByteArrayOutputStream;
import parquet.column.Encoding;
import parquet.column.values.ValuesWriter;
import parquet.column.values.bitpacking.ByteBitPackingLE;
import parquet.column.values.bitpacking.BytePacker;

public class DeltaBitPackingValuesWriter extends ValuesWriter {

	// use 32 as the buffer size because the packer is optimized to pack 32
	// values at a time
	private static final int BUFFER_SIZE = 32;

	private final CapacityByteArrayOutputStream cbaos;
	private final int[] buffer;
	private int bufferOffset;
	private int bitTally;
	private int lastNumber;
	private boolean isFirst;
	private BytePacker packer;
	private final byte[] temp;

	public DeltaBitPackingValuesWriter(int initialCapacity) {
		cbaos = new CapacityByteArrayOutputStream(initialCapacity);
		isFirst = true;

		// buffer up to BUFFER_SIZE integers
		buffer = new int[BUFFER_SIZE];

		// temp byte array to pack the buffer into
		temp = new byte[128];

		// current position within the buffer
		bufferOffset = 0;

		// running tally of the bits in the buffer
		bitTally = 0;
	}

	private void flushBuffer() {
		// determine how many bits are necessary to encode
		int maxBits = 32 - Integer.numberOfLeadingZeros(bitTally);

		// record down the max bit
		cbaos.write((byte) maxBits);

		// lookup the appropriate packer
		packer = ByteBitPackingLE.getPacker(maxBits);

		// pack the buffer into the temp byte array
		packer.pack32Values(buffer, 0, temp, 0);

		// write out the compressed data
		cbaos.write(temp, 0, bufferOffset * maxBits);

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
			// try to account for the buffered data, one byte is used to store the bit size
			int maxBits = (byte) (32 - Integer.numberOfLeadingZeros(bitTally));
			int packedSize = BytesUtils.paddedByteCountFromBits(bufferOffset
					* maxBits);
			return cbaos.size() + 1 + packedSize;
		}
	}

	@Override
	public BytesInput getBytes() {
		if (bufferOffset != 0) {
			flushBuffer();
		}
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
		cbaos.reset();
		isFirst = true;
		bufferOffset = 0;
		bitTally = 0;
	}

	@Override
	public void writeInteger(int v) {
		if (isFirst) {
			isFirst = false;
			cbaos.write(v);
		} else {
			// compute the delta, and zigzag encode it
			int delta = zigzagEncode(v - lastNumber);

			// store and increment offset
			buffer[bufferOffset++] = delta;

			// update the bit tally
			bitTally = bitTally | delta;
		}

		if (bufferOffset == BUFFER_SIZE) {
			// buffer is full, flush
			flushBuffer();
		}
		lastNumber = v;
	}

	private int zigzagEncode(int v) {
		if (v > 0) {
			return 2 * v - 1;
		} else {
			return -2 * v;
		}
	}
}
