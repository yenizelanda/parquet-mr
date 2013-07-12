package parquet.column.values.delta;

import java.io.IOException;

import parquet.column.values.ValuesReader;
import parquet.column.values.bitpacking.ByteBitPackingLE;
import parquet.column.values.bitpacking.BytePacker;

public class DeltaBitPackingValuesReader extends ValuesReader {

	private boolean isFirst;
	private int lastNumber;
	private int nextNumber;
	private byte[] currentPage;
	private int currentOffset;
	private final int[] buffer;
	private int bufferOffset;
	private byte maxBits;
	private BytePacker packer;

	public DeltaBitPackingValuesReader() {
		buffer = new int[32];
	}

	private void flushToBuffer() {
		maxBits = currentPage[currentOffset++];

		packer = ByteBitPackingLE.getPacker(maxBits);

		packer.unpack32Values(currentPage, currentOffset, buffer, 0);

		currentOffset += 4 * maxBits;

		bufferOffset = 0;
	}

	@Override
	public int initFromPage(long valueCount, byte[] page, int offset)
			throws IOException {
		currentPage = page;
		currentOffset = offset;
		isFirst = true;
		bufferOffset = 32;
		while (valueCount > 0) {
			offset += 1 + 4 * page[offset];
			valueCount -= 32;
		}

		return offset;
	}

	@Override
	public int readInteger() {

		if (bufferOffset == 32) {
			flushToBuffer();
		}

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

	private int zigzagDecode(int v) {
		if (v % 2 == 1) {
			return v / 2 + 1;
		} else {
			return -v / 2;
		}
	}

}
