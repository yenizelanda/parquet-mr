package parquet.column.values.varint;

/**
 * Helper class for group varint encoding. Provides zigzag functions and lookup tables.
 * @author Baris Kaya
 */

public class VarIntHelper {

	/**
	 * the lookup table which holds how many bytes is the next block of 4 numbers depending on firstByte
	 */
	public static int[] totalLength = new int[256];
	/**
	 * the lookup table which gives the specific length of the specific number, depending on firstByte
	 */
	public static int[][] length = new int[256][4];
	{
		for (int i = 0; i < 256; i++) {
			totalLength[i] = 0;
			for (int j = 0; j < 4; j++) {
				length[i][3 - j] = ((i >> (2 * j)) & 3) + 1;
				totalLength[i] += length[i][3 - j];
			}
		}
	};
	
	/**
	 * The function which encodes the difference between two numbers to a single
	 * positive integer.
	 * 
	 * @param v
	 *            the difference between the two integers
	 * @return the encoded number
	 */
	public static int encode(int v) {
		return (v << 1) ^ (v >> 31);
	}

	/**
	 * The function which decodes the difference between two numbers back to its
	 * original states.
	 * 
	 * @param v
	 *            the encoded number
	 * @return the difference between two integers
	 */
	public static int decode(int v) {
		return (v >>> 1) ^ (-(v & 1));
	}

}
