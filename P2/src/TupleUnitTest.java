import java.util.ArrayList;

public class TupleUnitTest {
	
	public static void main(String[] args) {
		
		String[] strArr1 = new String[3];
		strArr1[0] = "1";
		strArr1[1] = "0.10";
		strArr1[2] = "\"abcd\"";
		
		String[] strArr2 = new String[3];
		strArr2[0] = "?var1:int";
		strArr2[1] = "?var2:float";
		strArr2[2] = "?var3:string";
		
		Tuple t1 = new Tuple(strArr1);
		Tuple t2 = new Tuple(strArr2);

		if (t1.equals(t2)) {
			System.out.println("t1 and t2 are equal");
		}
		else {
			System.out.println("t1 and t2 are not equal");
		}

		System.out.println("t1: " + t1.toString());
		System.out.println("t2: " + t2.toString());

		System.out.println("t1.hashCode(): " + t1.hashCode());
		System.out.println("t2.hashCode(): " + t2.hashCode());

		Host host1 = Host.readHost("h1,129.210.16.81,33266");
		Host host2 = Host.readHost("h1,129.210.81,33267");
		
		ArrayList<Host> hosts = new ArrayList<Host>();
		hosts.add(host1);
	
		if (hosts.contains(host2)) {
			System.out.println("host1 and host2 are equal");
		}
		else {
			System.out.println("host1 does not equal host2");
		}
	}

}
