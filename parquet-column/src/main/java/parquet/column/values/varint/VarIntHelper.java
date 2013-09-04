package parquet.column.values.varint;

/**
 * Helper class for group varint encoding. Provides zigzag functions and lookup tables.
 * @author Baris Kaya
 */

class VarIntHelper {

	/**
	 * the lookup table which holds how many bytes is the next block of 4 numbers depending on firstByte
	 */
	public static int[] totalLength = new int[256];
	/**
	 * the lookup table which gives the specific length of the specific number, depending on firstByte
	 */
	public static int[][] length = new int[256][4];
	static
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
	 * The function which encodes a signed integer to an unsigned positive integer.
	 * 
	 * @param v
	 *            the difference between the two integers
	 * @return the encoded number
	 */
	public static int zigzagEncode(int v) {
		return (v << 1) ^ (v >> 31);
	}

	/**
	 * The function which decodes back a signed integer from an unsigned positive integer.
	 * 
	 * @param v
	 *            the encoded number
	 * @return the difference between two integers
	 */
	public static int zigzagDecode(int v) {
		return (v >>> 1) ^ (-(v & 1));
	}

}
