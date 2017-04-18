import java.io.*;

public class P1 {
	public static void main(String[] args) {
		
		if (args.length != 1) {
			System.out.println("Incorrect arguments, try again");
			return;
		}

		Server server = new Server(args[0]);
	}
}
