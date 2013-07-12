package parquet.column.values.delta;

import java.io.IOException;

import parquet.bytes.BytesInput;
import parquet.bytes.BytesUtils;
import parquet.bytes.CapacityByteArrayOutputStream;
import parquet.column.Encoding;
import parquet.column.values.ValuesWriter;
import parquet.io.ParquetDecodingException;

public class DeltaValuesWriter extends ValuesWriter {

	private final CapacityByteArrayOutputStream cbaos;
	private int lastNumber;
	private boolean isFirst;

	public DeltaValuesWriter(int initialCapacity) {
		cbaos = new CapacityByteArrayOutputStream(initialCapacity);
		isFirst = true;
	}

	@Override
	public long getAllocatedSize() {
		return cbaos.getCapacity();
	}

	@Override
	public long getBufferedSize() {
		return cbaos.size();
	}

	@Override
	public BytesInput getBytes() {
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
	}

	@Override
	public void writeInteger(int v) {
		if (isFirst) {
			isFirst = false;
			try {
				BytesUtils.writeIntLittleEndian(cbaos, v);
			} catch (IOException e) {
				throw new ParquetDecodingException("could not write int", e);
			}
		} else {
			try {
				BytesUtils.writeIntLittleEndian(cbaos, zigzagEncode(v
						- lastNumber));
			} catch (IOException e) {
				throw new ParquetDecodingException("could not write int", e);
			}
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
