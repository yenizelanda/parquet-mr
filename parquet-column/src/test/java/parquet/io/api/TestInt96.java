package parquet.io.api;

import java.math.BigInteger;

import org.junit.Assert;
import org.junit.Test;

public class TestInt96 {

	
	public void compare(Int96 A, BigInteger B) {
		System.out.println(B);
		System.out.println(A);
		Assert.assertEquals(new BigInteger(A.toByteArray()).toString(), B.toString());
	}
	@Test
	public void testBasicOperations() {
		
		BigInteger A, B, C, D;
		
		Int96 a, b, c, d;
		
		A = new BigInteger("1112333943892432849");
		B = new BigInteger("1315333912873156849");
		
		a = new Int96(A);
		b = new Int96(B);
		
		c = a.add(b);
		C = A.add(B);
		
		D = new BigInteger("3");
		d = new Int96(D);


		compare(c, C);
		
		
		c = b.substract(a);
		
		C = B.subtract(A);

		compare(c, C);


		c = a.bitShiftLeft(13);
		C = A.shiftLeft(13);
		compare(c, C);
		
		c = d.bitShiftLeft(50);
		C = D.shiftLeft(50);
		compare(c, C);
		

		c = a.bitShiftRight(13);
		C = A.shiftRight(13);
		compare(c, C);
		
		c = d.bitShiftRight(50);
		C = D.shiftRight(50);
		compare(c, C);
		
		
		c = a.negate();
		C = A.negate();
		compare(c, C);
		
		
		
		
		
	}
}
