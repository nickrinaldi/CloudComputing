import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.security.MessageDigest;
import java.nio.ByteBuffer;
import java.util.HashSet;

/*
 * Server class that starts the host server and the client side prompt
 */
public class Server {
	
	private Host host;
	private ArrayList<Host> connectedHosts;
	private ArrayList<Tuple> tuples;
	// tupleLock to prevent race conditions
	private Object tupleLock = new Object();

	String filePath;

	public Server() {
	}

	public Server(String name) {
		this.host = new Host(name);
		this.connectedHosts = new ArrayList<Host>();
		this.tuples = new ArrayList<Tuple>();

		// upon creation, create the directories and clear the host and tuple files
		filePath = createDirectories();
		writeHosts();
		writeTuples();
		startServer();
	}

	public String getName() {
		return host.getName();
	}

	public InetSocketAddress getAddress() {
		return host.getAddress();
	}

	/*
	 * add hosts method that communicated with the other hosts to be added
	 */
	private void addHosts() {
		writeHosts();

		boolean connectionErr = false;

		// create arraylist of strings because arraylist of hosts is not serializable
		ArrayList<String> hostStrings = new ArrayList<String>();
		for (int i = 0; i < connectedHosts.size(); i++) {
			hostStrings.add(connectedHosts.get(i).toString());
		}

		for (int i = 0; i < connectedHosts.size(); i++) {
			Host connectedHost = connectedHosts.get(i);
			
			if (host.equals(connectedHost)) {
				// don't communicate if connected host is the same host
				// initiating the command
				continue;
			}

			// send the list over
			try {
				int connectedHostPort = connectedHost.getPort();
				InetAddress connectedHostAddress = connectedHost.getIPAddress();

				Socket socket = new Socket(connectedHostAddress, connectedHostPort);

				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
				out.writeObject(hostStrings);

				socket.close();

			} catch (Exception e) {
				System.out.println("connection error");
				connectedHosts.remove(i);
				connectionErr = true;
				e.printStackTrace();
				break;
			}
		}

		if (connectionErr) {
			addHosts();
		}
	}

	/*
	 * evaluates the add host command
	 */
	private String addHostCommand(String command) {
		String[] hosts = LindaInputParser.getHosts(command);
		boolean addedHost = false;

		if (hosts.length == 0) {
			return "-linda: invalid command arguments";
		}

		String result = "";

		for (int i = 0; i < hosts.length; i++) {
			Host h = Host.readHost(hosts[i]);

			if (connectedHosts.contains(h)) {
				result = "host: " + h.getName() + " already exists";
			}
			else {
				connectedHosts.add(h);
				addedHost = true;
			}
		}

		if (addedHost) {
			addHosts();
		}
		return result;
	}

	/*
	 * update the host file
	 */
	private void updateHosts(ArrayList<String> hosts) {
		connectedHosts = new ArrayList<Host>();

		for (int i = 0; i < hosts.size(); i++) {
			connectedHosts.add(Host.readHost(hosts.get(i)));
		}

		writeHosts();
	}


	private String createDirectories() {
		String result;

		String location = "/tmp/nrinaldi";
		File f = new File(location);
		f.mkdir();
		f.setExecutable(true, false);
		f.setReadable(true, false);
		f.setWritable(true, false);

		location += "/linda";
		f = new File(location);
		f.mkdir();
		f.setExecutable(true, false);
		f.setReadable(true, false);
		f.setWritable(true, false);
		
		location += ("/" + host.getName());
		f = new File(location);
		f.mkdir();
		f.setExecutable(true, false);
		f.setReadable(true, false);
		f.setWritable(true, false);

		result = location;
	
		String netsLoc = location + "/nets";
		String tuplesLoc = location + "/tuples";
		f = new File(netsLoc);
		try {
			f.createNewFile();
			f.setExecutable(false, false);
			f.setReadable(true, false);
			f.setWritable(true, false);
			f = new File(tuplesLoc);
			f.createNewFile();
			f.setExecutable(false, false);
			f.setReadable(true, false);
			f.setWritable(true, false);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return result;
	}

	/*
	 * create the nets file and write the hosts to it
	 */
	private void writeHosts() {
		// clear the file
		try {
			PrintWriter pw = new PrintWriter(filePath + "/nets");
			pw.close();
		} catch (FileNotFoundException e) {
			System.err.println("file not found");
			e.printStackTrace();
		}

		try {
				
			FileWriter netsWriter = new FileWriter(filePath + "/nets");
			BufferedWriter bw = new BufferedWriter(netsWriter);
			
			for (int i = 0; i < connectedHosts.size(); i++) {
				bw.write(connectedHosts.get(i).toString());
				bw.newLine();
			}

			bw.close();
			netsWriter.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}	
	}

	/*
	 * create the tuples file and write the tuples to it
	 */
	private void writeTuples() {
		// clear the file
		try {
			PrintWriter pw = new PrintWriter(filePath + "/tuples");
			pw.close();
		} catch (FileNotFoundException e) {
			System.err.println("file not found");
			e.printStackTrace();
		}

		try {
				
			FileWriter tupsWriter = new FileWriter(filePath + "/tuples");
			BufferedWriter bw = new BufferedWriter(tupsWriter);
			
			for (int i = 0; i < tuples.size(); i++) {
				bw.write(tuples.get(i).toString());
				bw.newLine();
			}

			bw.close();
			tupsWriter.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}	
	}

	private void startServer() {
		try {
			ServerSocket serverSocket = new ServerSocket(0, 0, Inet4Address.getLocalHost());
			InetSocketAddress serverAddress = new InetSocketAddress(
					serverSocket.getInetAddress(), serverSocket.getLocalPort());
			host.setAddress(serverAddress);

			connectedHosts.add(host);

			// create client prompt thread
			(new Thread(new LindaPrompt(serverAddress))).start();

			while (true) {
				Socket clientSocket = serverSocket.accept();

				(new Thread(new ClientRequestHandler(clientSocket))).start();
			}

		} catch (IOException e) {
			System.err.println("unable to accept connections");
			e.printStackTrace();
		}
	}

	/*
	 * ClientRequestHandler class allows for the server to be multithreaded
	 * Handles incoming requests so the main server thread can continually accept
	 * connection requests. This also allows for easy blocking implementation
	 * without blocking the server.
	 */
	private class ClientRequestHandler extends Thread {
		private static final String N_ACK = "nack";
		private static final String ACK = "ack";
		
		ObjectOutputStream clientOut;
		ObjectInputStream clientIn;

		ClientRequestHandler() {
		}

		ClientRequestHandler(Socket clientSocket) {
			try {
				clientOut = new ObjectOutputStream(clientSocket.getOutputStream());
				clientIn = new ObjectInputStream(clientSocket.getInputStream());
			
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		@SuppressWarnings("unchecked")
		public void run() {
			try {
				Object object = clientIn.readObject();

				if (object instanceof String) {
					String input = (String) object;
					inputHandler(input);
				}
				else if (object instanceof ArrayList) {
					ArrayList<String> hosts = (ArrayList<String>)object;
					updateHosts(hosts);
				}

			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}

		private void inputHandler(String input) {
			String message = "";
			Matcher matcher = Pattern.compile(
					"(\\w+)").matcher(input);

			if (!matcher.find()) {
				message = "-linda: invalid command";
				try {
					clientOut.writeObject(message);
				} catch (Exception e) {
					e.printStackTrace();
				}

				return;
			}

			// evaluate the command
			try {
				switch(matcher.group(1)) {
					case "tuple":
						synchronized(tupleLock) {
							tupleLock.notifyAll();
						}
						break;
					case "add":
						message = addHostCommand(input);
						clientOut.writeObject(message);
						break;
					case "remove_tuple":
						getTuple(input, true);
						break;
					case "get_tuple":
						getTuple(input, false);
						break;
					case "contains_tuple":
						containsTuple(input);
						break;
					case "in":
						inCommand(input);
						break;
					case "out":
						outCommand(input);
						break;
					case "rd":
						rdCommand(input);
						break;
					default:
						message = "-linda: invalid command";
						clientOut.writeObject(message);
						break;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

		private void inCommand(String command) {
			getTuple(command, true);
		}

		private void rdCommand(String command) {
			getTuple(command, false);
		}

		/*
		 * puts a tuple in the tuple store
		 */
		private void outCommand(String command) {
			String message = "";
			Tuple tuple = new Tuple(LindaInputParser.parseTuple(command));
		
			// hash it % number of connected hosts
			int hostIndex = tuple.hashCode() % connectedHosts.size();
			Host h = connectedHosts.get(hostIndex);

			try {
				InetAddress address = h.getIPAddress();
				int port = h.getPort();

				message = "put tuple (" + tuple.toString() +
						") on " + address.getHostAddress();

				if (host.equals(h)) {
					synchronized(tupleLock) {
						tuples.add(tuple);
						writeTuples();
						tupleLock.notifyAll();
					}
					
					// notify all other hosts
					for (int i = 0; i < connectedHosts.size(); i++) {
						try {
							Host connectedHost = connectedHosts.get(i);
							if (connectedHost.equals(host)) {
								continue;
							}
							Socket socket = new Socket(connectedHost.getIPAddress(), connectedHost.getPort());
							ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
							oos.writeObject(new String("tuple"));
							socket.close();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}

				}

				else {
					Socket socket = new Socket(address, port);
					ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
					ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());

					oos.writeObject(command);
					Object o = ois.readObject();

					if (o instanceof String) {
						message = (String) o;
					}

					socket.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

			try {
				clientOut.writeObject(message);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		/*
		 * contains tuple module for servers to communicate between each other
		 */
		private void containsTuple(String command) {
			Tuple tuple = new Tuple(LindaInputParser.parseTupleQuery(command));
			String message = "";
			int index = -1;
			synchronized(tupleLock) {
				for (int i = 0; i < tuples.size(); i++) {
					Tuple t = tuples.get(i);
					if (tuple.equals(t)) {
						index = i;
						message = "(" + t.toString() + ")";
					}
				}

				try {
					clientOut.writeObject(message);
					Object o = clientIn.readObject();
			
					if ((o instanceof String) && ((String) o).equals(ACK) && index >= 0) {
						tuples.remove(index);
						writeTuples();
					}

				} catch (Exception e) {
					e.printStackTrace();
				}
			}

		}
		
		/*
		 * gets a tuple from the tuple store, second argument determines whether the tuple
		 * must be removed or not
		 */
		private void getTuple(String command, boolean removeTuple) {
			Tuple tuple = new Tuple(LindaInputParser.parseTupleQuery(command));
			String message = "";

			if (!tuple.containsQuery()) {
				// we can simply hash it
				int hostIndex = tuple.hashCode() % connectedHosts.size();
				Host h = connectedHosts.get(hostIndex);

				try {
					InetAddress address = h.getIPAddress();
					int port = h.getPort();

					if (host.equals(h)) {
						Tuple result = new Tuple();
						synchronized(tupleLock) {
							while (!tuples.contains(tuple)) {
								tupleLock.wait();
							}
							int index = tuples.indexOf(tuple);
							result = (index > -1) ? tuples.get(index) : new Tuple();

							if (removeTuple && index > -1) {
								tuples.remove(index);
								writeTuples();
							}
						}

						message = "get tuple (" + result.toString() + ") on " +
							host.getIPAddress().getHostAddress();
					}
					else {
						Socket socket = new Socket(address, port);
						ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
						ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
						
						if (removeTuple) {
							oos.writeObject("remove_tuple(" + tuple.toString() + ")");
						}
						else {
							oos.writeObject("get_tuple(" + tuple.toString() + ")");
						}
						
						Object o = ois.readObject();

						if (o instanceof String) {
							message = (String) o;
						}

						socket.close();
					}

					clientOut.writeObject(message);
					return;
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			else {
				// the tuple has a variable in it
				Tuple result = null;
				InetAddress ipAddr = host.getIPAddress();
				int port = host.getPort();

				try {
				synchronized(tupleLock) {
					// test if the tuple is stored locally
					while (result == null) {
						for (int i = 0; i < tuples.size(); i++) {
							if (tuple.equals(tuples.get(i))) {
								result = tuples.get(i);

								if (removeTuple) {
									tuples.remove(i);
									writeTuples();
								}

								break;
							}
						}
						if (result == null) {
							// test if the tuple is stored on a separate host
							for (int i = 0; i < connectedHosts.size(); i++) {
								Host h = connectedHosts.get(i);
								if (host.equals(h)) {
									continue;
								}
								Socket socket = new Socket(h.getIPAddress(), h.getPort());
								ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
								ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
								oos.writeObject("contains_tuple(" + tuple.toString() + ")");
								
								Object o = ois.readObject();
								if (removeTuple) {
									oos.writeObject(ACK);
								}
								else {
									oos.writeObject(N_ACK);
								}

								socket.close();

								if (o instanceof String) {
									message = (String) o;
								
									if (message.length() > 0) {
										ipAddr = h.getIPAddress();
										port = h.getPort();
										result = new Tuple(LindaInputParser.parseTuple(message));
										break;
									}
								}
							}
						}
						if (result == null) {
							// tuple doesn't exist, block
							tupleLock.wait();
						}
					}
				}
				} catch (Exception e) {
					e.printStackTrace();
				}

				message = "get tuple (" + result.toString() + ") on " + ipAddr.getHostAddress();
				try {
					clientOut.writeObject(message);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
}
