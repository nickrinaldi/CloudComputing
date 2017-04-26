
public class ConsistentHashingUnitTest {
	
	private static final Long LARGE_PRIME =  948701839L;
    private static final Long LARGE_PRIME2 = 6920451961L;
	
	public static void main(String[] args) {
		
		String[] strArr1 = new String[3];
		strArr1[0] = "1";
		strArr1[1] = "0.10";
		strArr1[2] = "\"abcd\"";
		
		Tuple t1 = new Tuple(strArr1);
		//System.out.println(t1.hashCode());
		
		int[] lookupTable = new int[(int) Math.pow(2, 16)];

		int tableSize = lookupTable.length;
		int numberCurrentHosts = 1;
		int numberNewHosts = 2;
		int partition = (tableSize / numberCurrentHosts) / numberNewHosts;
		
		int oldIndex = -1;
		for (int i = 0; i < lookupTable.length; i++) {
			oldIndex = i / partition;
			if (oldIndex >= numberNewHosts) {
				oldIndex--;
			}
			
			lookupTable[i] = oldIndex;
		}
		
		numberCurrentHosts++;
		numberNewHosts++;
		partition = tableSize / (numberCurrentHosts * numberNewHosts);
		
		int[] hostBuckets = new int[2];
		hostBuckets[0] = 1;
		hostBuckets[1] = 1;
		
		int minIndex, maxIndex;
		oldIndex = -1;
		for (int i = 0; i < lookupTable.length; i++) {
			if (lookupTable[i] != oldIndex) {
				minIndex = i;
			}
		}
		
//		int partition = 0;
//		int i = 0;
//		int hostIndex = 0;
//		double numberCurrentHosts = 1;
//		double numberNewHosts = 4;
//		double tableSize = lookupTable.length;
//		
//		while (i < lookupTable.length) {
//			tableSize = tableSize - partition;
//			partition = (int) ((tableSize / numberCurrentHosts) / numberNewHosts + 0.5);
//			System.out.println("partition: " + partition);
//			
//			for (int j = 0; j < partition && i < lookupTable.length; j++) {
//				lookupTable[i] = hostIndex;
//				System.out.println(lookupTable[i]);
//				i++;
//			}
//			
//			hostIndex++;
//			numberNewHosts--;
//		}
		
	}
	
	public static int hash(int i) {
	    // Spread out values
	    long scaled = (long) i * LARGE_PRIME;

	    // Fill in the lower bits
	    long shifted = scaled + LARGE_PRIME2;

	    // Add to the lower 32 bits the upper bits which would be lost in
	    // the conversion to an int.
	    long filled = shifted + ((shifted & 0xFFFFFFFF00000000L) >> 32);

	    // Pare it down to 31 bits in this case.  Replace 7 with F if you
	    // want negative numbers or leave off the `& mask` part entirely.
	    int masked = (int) (filled & 0x7FFFFFFF);
	    return masked % (int) Math.pow(2, 16);
	}
}
