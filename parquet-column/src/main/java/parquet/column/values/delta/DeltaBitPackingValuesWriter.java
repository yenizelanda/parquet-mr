package parquet.column.values.delta;



import parquet.bytes.BytesInput;
import parquet.bytes.CapacityByteArrayOutputStream;
import parquet.column.Encoding;
import parquet.column.values.ValuesWriter;
import parquet.column.values.bitpacking.ByteBitPackingLE;
import parquet.column.values.bitpacking.BytePacker;
import static parquet.column.Encoding.RLE;

public class DeltaBitPackingValuesWriter extends ValuesWriter {

	private CapacityByteArrayOutputStream cbaos;
	private int[] buffer;
	private int bufferOffset;
	private int byteCap;
	private byte maxBits;
	private int lastNumber;
	private boolean isFirst;
	private BytePacker packer;
	private byte[] temp;

	public DeltaBitPackingValuesWriter(int initialCapacity) {
		cbaos = new CapacityByteArrayOutputStream(initialCapacity);
		isFirst = true;
		buffer = new int[32];
		temp = new byte[128];
		bufferOffset = 0;
		byteCap = 0;
	}

	@Override
	public long getBufferedSize() {
		return cbaos.size() + 1 + 4 * (32 - Integer.numberOfLeadingZeros(byteCap));
	}

	@Override
	public BytesInput getBytes() {
		if (bufferOffset != 0)
			flushBuffer();
		return BytesInput.from(cbaos);
	}

	@Override
	public Encoding getEncoding() {
		return RLE;
	}

	@Override
	public void reset() {
		cbaos.reset();
		isFirst = true;
		bufferOffset = 0;
		byteCap = 0;
		
	}

	@Override
	public long getAllocatedSize() {
		return cbaos.getCapacity();
	}

	@Override
	public String memUsageString(String prefix) {
		return cbaos.memUsageString(prefix);
	}

	private int zigzagEncode(int v) {
		if (v > 0)
			return 2 * v - 1;
		else
			return -2 * v;
	}

	private void flushBuffer()
	{
		
		maxBits = (byte) (32 - Integer.numberOfLeadingZeros(byteCap));
		
		cbaos.write(maxBits);

		packer = ByteBitPackingLE.getPacker(maxBits);
		
		packer.pack32Values(buffer, 0, temp, 0);
		
		cbaos.write(temp, 0, 4 * maxBits);
		
		bufferOffset = 0;
		byteCap = 0;
	}
	
	@Override
	public void writeInteger(int v) {
		if (isFirst) {
			isFirst = false;
			buffer[bufferOffset++] = v;
		} else
			buffer[bufferOffset++] = zigzagEncode(v - lastNumber);

		byteCap = byteCap | buffer[bufferOffset - 1];
		
		if (bufferOffset == 32)
			flushBuffer();
		lastNumber = v;
	}
}
