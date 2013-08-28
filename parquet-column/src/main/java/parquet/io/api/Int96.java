package parquet.io.api;

import java.math.BigInteger;

public class Int96 {

	long lowPart, highPart;
	
	private static long bitMask48Bit = (1L << 48) - 1;
	
	public Int96() {
		this(0,0);
	}
	
	public Int96(String input) {
		Int96 now = new Int96();
		boolean negate = false;
		int i = 0;
		if (input.charAt(0) == '-') {
			negate = true;
			i = 1;
		}
		
		for (; i < input.length(); i++) {
			now = now.multiply(10);
			now = now.add(input.charAt(i) - '0');
		}
		
		if (negate) now = now.negate();

		lowPart = now.lowPart;
		highPart = now.highPart;
	}
	
	public Int96(byte[] value) {
		this(value, 0, value.length);
	}
	
	public Int96(BigInteger input) {
		this(input.toString());
	}
	
	public Int96(byte[] value, int offset, int length) {
		Int96 ret = new Int96();
		for (int i = offset; i < offset + length; i++) {
			ret = ret.bitShiftLeft(8);
			ret = ret.add((value[i] + 256) % 256);
		}
		lowPart = ret.lowPart;
		highPart = ret.highPart;
	}
	
	public Int96(long lowPart, long highPart) {
		this.lowPart = lowPart;
		this.highPart = highPart;
		
	}
	
	public Int96(Int96 other) {

		this.lowPart = other.lowPart;
		this.highPart = other.highPart;

	}
	
	public void format() {
		lowPart = lowPart & bitMask48Bit;
		highPart = highPart & bitMask48Bit;
		
	}
	
	public Int96 add(Int96 other) {
		
		Int96 ret = new Int96(other);

		ret.lowPart += this.lowPart;
		ret.highPart += this.highPart + (ret.lowPart >> 48);
		
		ret.format();
		return ret;
	}
	
	public Int96 multiply(int other) {
		Int96 ret = new Int96();

		ret.lowPart = this.lowPart * other;
		ret.highPart = this.highPart * other + (ret.lowPart >> 48);
		ret.format();
		return ret;	
	}
	
	public Int96 add(int other) {
		Int96 ret = new Int96();
		
		ret.lowPart = this.lowPart + other;
		ret.highPart = this.highPart + (ret.lowPart >> 48);
		ret.format();
		
		return ret;
	}
	
	public Int96 negate() {
		Int96 ret = new Int96(this);
		
		ret.lowPart = ret.lowPart ^ bitMask48Bit;
		ret.highPart = ret.highPart ^ bitMask48Bit;
		
		return ret.add(1);
	}
	
	public Int96 substract(Int96 other) {
		return this.add(other.negate());
	}
	
	public Int96 bitShiftLeft(int other) {
		Int96 ret = new Int96(this);
		long addend;
		if (48 > other) addend = ret.lowPart >> (48 - other);
		else addend = ret.lowPart << (other - 48);
		ret.lowPart = ret.lowPart << other;
		
		ret.highPart = (ret.highPart << other) + addend;
		
		ret.format();

		return ret;
	}
	
	public Int96 bitShiftRight(int other) {
		Int96 ret = new Int96(this);
		
		ret.lowPart = (ret.lowPart >> other) + ((((1L << other) - 1) & ret.highPart) << (48 - other));
		ret.highPart = ret.highPart >> other;

		ret.format();
		
		return ret;
	}
	
	public byte[] toByteArray() {
		byte[] ret = new byte[12];
		
		for (int i = 0; i < 6; i++) {
			ret[i] = (byte) ((highPart >> (8 * (5 - i))) & 255);
		}
		
		for (int i = 0; i < 6; i++) {
			ret[6 + i] = (byte) ((lowPart >> (8 * (5 - i))) & 255);
		}
		
		return ret;
	}
	
	public void print() {
		byte[] yo = toByteArray();
		for (int i = 0; i < 12; i++)
			System.out.print(yo[i] + " ");
		System.out.println();
	}

	/**
	 * @return the lowPart
	 */
	public long getLowPart() {
		return lowPart;
	}

	/**
	 * @return the highPart
	 */
	public long getHighPart() {
		return highPart;
	}
	
	public String toString() {
		return new BigInteger(toByteArray()).toString();
	}

	
	
	
}
