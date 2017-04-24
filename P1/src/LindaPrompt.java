import java.io.*;
import java.net.*;

public class LindaPrompt implements Runnable {
	InetSocketAddress serverAddress;

	public LindaPrompt(InetSocketAddress serverAddress) {
		this.serverAddress = serverAddress;
	}

	public void run() {
		startPrompt();
	}

	private String instructServer(String command) {
		String response = "";
		
		try {
			Socket clientSocket = new Socket(serverAddress.getAddress(),
					serverAddress.getPort());
			
			ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
			ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream());

			out.writeObject(command);
			Object o = in.readObject();

			if (o instanceof String) {
				response = (String) o;
			}

			clientSocket.close();

		} catch (Exception e) {
			System.err.println("error connecting to server");
			e.printStackTrace();
		}

		return response;

	}

	private void startPrompt() {
		System.out.println(serverAddress.getAddress().getHostAddress() +
				" at port number " + serverAddress.getPort());
		System.out.print("linda> ");

		try {
			BufferedReader stdIn = new BufferedReader(
					new InputStreamReader(System.in));
			String userInput;

			while ((userInput = stdIn.readLine()) != null) {
				String response = instructServer(userInput);
				
				if (response.length() > 0) {
					System.out.println(response);
				}

				System.out.print("linda> ");
			}

		} catch (IOException e) {
			System.err.println("unable to get input");
			e.printStackTrace();
		}
	}
}
