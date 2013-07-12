package parquet.column.values.delta;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import parquet.Ints;
import parquet.bytes.BytesUtils;
import parquet.column.values.ValuesReader;
import parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import parquet.io.ParquetDecodingException;

public class DeltaHybridValuesReader extends ValuesReader {

	private final int bitWidth;
	private RunLengthBitPackingHybridDecoder decoder;
	private boolean isFirst;
	private int lastNumber;
	private int nextNumber;

	public DeltaHybridValuesReader() {
		this.bitWidth = 32;
		isFirst = true;
	}

	@Override
	public int initFromPage(long valueCountL, byte[] page, int offset)
			throws IOException {
		// TODO: we are assuming valueCount < Integer.MAX_VALUE
		// we should address this here and elsewhere
		int valueCount = Ints.checkedCast(valueCountL);
		isFirst = true;

		if (valueCount <= 0) {
			// readInteger() will never be called,
			// there is no data to read
			return offset;
		}

		ByteArrayInputStream in = new ByteArrayInputStream(page, offset,
				page.length);
		int length = BytesUtils.readIntLittleEndian(in);

		decoder = new RunLengthBitPackingHybridDecoder(bitWidth, in);

		// 4 is for the length which is stored as 4 bytes little endian
		return offset + length + 4;
	}

	@Override
	public int readInteger() {
		if (isFirst) {
			try {
				nextNumber = decoder.readInt();
			} catch (IOException e) {
				throw new ParquetDecodingException("could not read int", e);
			}
			lastNumber = nextNumber;
			isFirst = false;
			return nextNumber;
		} else {
			try {
				nextNumber = lastNumber + zigzagDecode(decoder.readInt());
			} catch (IOException e) {
				throw new ParquetDecodingException("could not read int", e);
			}
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
