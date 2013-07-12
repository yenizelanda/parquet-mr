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

/**
 * The helper library for delta encoding classes.
 * 
 * @author Baris Kaya
 * 
 */
public class DeltaEncoding {

	/**
	 * The function which encodes the difference between two numbers to a single
	 * positive integer.
	 * 
	 * @param v
	 *            the difference between the two integers
	 * @return the encoded number
	 */
	public static int zigzagEncode(int v) {
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
	public static int zigzagDecode(int v) {
		return (v >>> 1) ^ (-(v & 1));
	}

	/**
	 * The function which encodes the difference between two numbers to a single
	 * positive integer.
	 * 
	 * @param v
	 *            the difference between the two integers
	 * @return the encoded number
	 * @deprecated
	 */
	public static int zigzagDecodeOld(int v) {
		if (v % 2 == 1) {
			return v / 2 + 1;
		} else {
			return -v / 2;
		}
	}

	/**
	 * The function which decodes the difference between two numbers back to its
	 * original states.
	 * 
	 * @param v
	 *            the encoded number
	 * @return the difference between two integers
	 * @deprecated
	 */
	public static int zigzagEncodeOld(int v) {
		if (v > 0) {
			return 2 * v - 1;
		} else {
			return -2 * v;
		}
	}
}
