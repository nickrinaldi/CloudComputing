public class P2 {
	public static void main(String[] args) {
		
		if (args.length != 1) {
			System.out.println("Incorrect arguments, try again");
			return;
		}

		new Server(args[0]);
	}
}
