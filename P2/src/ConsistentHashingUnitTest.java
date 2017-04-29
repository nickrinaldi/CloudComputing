
public class ConsistentHashingUnitTest {
	
	public static void main(String[] args) {
		
		String[] strArr1 = new String[3];
		strArr1[0] = "1";
		strArr1[1] = "0.10";
		strArr1[2] = "\"abcd\"";
		
		Tuple t1 = new Tuple(strArr1);
		System.out.println(t1.hashCode());
		
	}
}
