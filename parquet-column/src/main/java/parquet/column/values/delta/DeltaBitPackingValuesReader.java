package parquet.column.values.delta;

import java.io.IOException;

import parquet.bytes.BytesUtils;
import parquet.column.values.ValuesReader;
import parquet.column.values.bitpacking.ByteBitPackingLE;
import parquet.column.values.bitpacking.BytePacker;
import parquet.io.ParquetDecodingException;

public class DeltaBitPackingValuesReader extends ValuesReader {

	private boolean isFirst;
	private int lastNumber;
	private int nextNumber;
	private byte[] b;
	private int currentOffset;
	private int[] buffer;
	private int bufferOffset;
	private byte maxBits;
	private BytePacker packer;

	public DeltaBitPackingValuesReader() {
		buffer = new int[32];
	}

	@Override
	public int initFromPage(long valueCount, byte[] page, int offset)
			throws IOException {
		b = page;
		currentOffset = offset;
		isFirst = true;
		bufferOffset = 32;
		while (valueCount > 0) {
			offset += 1 + 4 * page[offset];
			valueCount -= 32;
		}

		return offset;
	}

	private int zigzagDecode(int v) {
		if (v % 2 == 1) {
			return v / 2 + 1;
		} else
			return -v / 2;
	}

	private void flushToBuffer() {
		maxBits = b[currentOffset++];

		packer = ByteBitPackingLE.getPacker(maxBits);

		packer.unpack32Values(b, currentOffset, buffer, 0);

		currentOffset += 4 * maxBits;

		bufferOffset = 0;
	}

	@Override
	public int readInteger() {

		if (bufferOffset == 32)
			flushToBuffer();

		if (isFirst) {
			nextNumber = buffer[bufferOffset++];
			lastNumber = nextNumber;
			isFirst = false;
			return nextNumber;
		} else {
			nextNumber = lastNumber + zigzagDecode(buffer[bufferOffset++]);
			lastNumber = nextNumber;
			return nextNumber;
		}
	}

}
