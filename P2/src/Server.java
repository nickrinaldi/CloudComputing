import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*
 * Server class that starts the host server and the client side prompt
 */
public class Server {
//	private static final String DIRECTORY_PREFIX =
//			"C:/Users/Nick/Documents/GitHub/CloudComputing/P2/";
	private static final String DIRECTORY_PREFIX =
			"C:/Users/User/Documents/GitHub/CloudComputing/P2/";
	
	private Host host;
	private ArrayList<Host> connectedHosts;
	private ArrayList<Tuple> tuples;
	// tupleLock to prevent race conditions for client handlers
	private Object tupleLock = new Object();
	// hostLock to prevent race conditions for client handlers
	//private Object hostLock = new Object();
	private int[] lookupTable;
	
	String filePath;

	public Server() {
	}

	public Server(String name) {
		this.host = new Host(name);
		this.connectedHosts = new ArrayList<Host>();
		this.tuples = new ArrayList<Tuple>();
		this.lookupTable = new int[(int) Math.pow(2, 16)];
		
		// upon creation, create the directories and clear the host and tuple files
		filePath = createDirectories();
//		writeHosts();
//		writeTuples();
		startServer();
	}

	public String getName() {
		return host.getName();
	}

	public InetSocketAddress getAddress() {
		return host.getAddress();
	}

	/*
	 * add hosts method that communicated with the other hosts to be added, the hosts
	 * in the parameter are unique and not connected
	 */
	private void addHosts(Host[] hosts) {
		int tableSize = lookupTable.length;
		int numberCurrentHosts = connectedHosts.size();
		int numberNewHosts = connectedHosts.size() + hosts.length;
		
		synchronized (connectedHosts) {
			boolean addedHost = false;
			
			for (int i = 0; i < hosts.length; i++) {
				if (!hosts[i].equals(host)) {
					addedHost = true;
					connectedHosts.add(hosts[i]);
				}
			}
			
			int partition = (tableSize / numberCurrentHosts) / numberNewHosts;
			for (int i = 0; i < tableSize; i++) {
				lookupTable[i] = i / partition;
			}
			
			if (addedHost) {
				writeHosts();
				broadcastObject(buildHostStrings(connectedHosts));
			}
		}
	}

	/*
	 * delete the hosts and redistribute the tuples
	 */
	private void deleteHosts(Host[] hosts) {
		synchronized (connectedHosts) {
			boolean removedHost = false;
			boolean removeSelf = false;
			
			ArrayList<Host> hostList = new ArrayList<Host>(connectedHosts);
			
			for (int i = 0; i < hosts.length; i++) {
				if (hostList.remove(hosts[i])) {
					removedHost = true;
				}
				if (hosts[i].equals(host)) {
					removeSelf = true;
				}
			}
			
			if (removedHost) {
				broadcastObject(buildHostStrings(hostList));
			}
			
			connectedHosts = hostList;
			
			if (removeSelf) {
				connectedHosts.clear();
			}
			writeHosts();
		}
	}

	/*
	 * put tuple in the tuple store
	 */
	private void putTuple(Tuple tuple) {
		synchronized (tuples) {
			tuples.add(tuple);
			writeTuples();
		}
	}
	
	/*
	 * delete tuple in the tuple store
	 */
	private void deleteTuple(Tuple tuple) {
		synchronized (tuples) {
			tuples.remove(tuple);
			writeTuples();
		}
	}
	
	/*
	 * contains tuple module for servers to communicate between each other
	 */
	private Tuple containsTuple(Tuple tuple) {
		synchronized(tuples) {
			int index = tuples.indexOf(tuple);
			
			if (index >= 0 && index < tuples.size()) {
				return tuples.get(index);
			}
			
			return null;
		}
	}
	
	/*
	 * broadcast the object to the connected hosts
	 */
	private void broadcastObject(Object o) {
		Iterator<Host> iterator = connectedHosts.iterator();
		
		while (iterator.hasNext()) {
			Host connectedHost = iterator.next();
			
			if (host.equals(connectedHost)) {
				// don't communicate if connected host is the same host
				continue;
			}

			// send the object over
			try {
				int connectedHostPort = connectedHost.getPort();
				InetAddress connectedHostAddress = connectedHost.getIPAddress();

				Socket socket = new Socket(connectedHostAddress, connectedHostPort);

				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
				ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
				
				out.writeObject(o);
				in.close();
				out.close();
				socket.close();

			} catch (Exception e) {
				System.out.println("error connecting to host: " + connectedHost.getName());
			}
		}
	}

	private ArrayList<String> buildHostStrings(ArrayList<Host> hosts) {
		// create arraylist of strings because arraylist of hosts is not serializable
		ArrayList<String> hostStrings = new ArrayList<String>();
		for (int i = 0; i < hosts.size(); i++) {
			hostStrings.add(hosts.get(i).toString());
		}

		return hostStrings;
	}

	/*
	 * update the host file
	 */
	private void updateHosts(ArrayList<String> hosts) {
		synchronized (connectedHosts) {
			connectedHosts = new ArrayList<Host>();
	
			for (int i = 0; i < hosts.size(); i++) {		
				connectedHosts.add(Host.readHost(hosts.get(i)));
			}
			
			// if self is not contained in the list
			if (!connectedHosts.contains(host)) {
				connectedHosts.clear();
			}
	
			writeHosts();
		}
	}


	private String createDirectories() {
		String result;

		String location = DIRECTORY_PREFIX + "tmp/nrinaldi";
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
			
			FileReader fr = new FileReader(netsLoc);
			BufferedReader br = new BufferedReader(fr);
			String line;
			while ((line = br.readLine()) != null) {
				connectedHosts.add(Host.readHost(line));
			}
			if (br != null) {
				br.close();
			}
			if (fr != null) {
				fr.close();
			}
			
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

	/*
	 * essentially the main function of the server, waits for connections then
	 * spawns thread to deal with those connections
	 */
	private void startServer() {
		ServerSocket serverSocket = null;
		try {
			serverSocket = new ServerSocket(0, 0, Inet4Address.getLocalHost());
			InetSocketAddress serverAddress = new InetSocketAddress(
					serverSocket.getInetAddress(), serverSocket.getLocalPort());
			host.setAddress(serverAddress);

			if (connectedHosts.size() == 0) {
				// initial startup
				connectedHosts.add(host);
			}
			else {
				// startup after crash, has connected hosts
				synchronized (connectedHosts) {
					int index = connectedHosts.indexOf(host);
					if (index >= 0 && index < connectedHosts.size()) {
						connectedHosts.get(index).setAddress(serverAddress);
						writeHosts();
						broadcastObject(buildHostStrings(connectedHosts));
					}
				}
			}

			// create client prompt thread
			(new Thread(new LindaPrompt(serverAddress))).start();

			while (true) {
				Socket clientSocket = serverSocket.accept();

				(new Thread(new ClientRequestHandler(clientSocket))).start();
			}

		} catch (IOException e) {
			System.err.println("unable to accept connections");
			e.printStackTrace();
		} finally {
			try {
				serverSocket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
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
		
		Socket clientSocket;
		ObjectInputStream clientIn;
		ObjectOutputStream clientOut;

		ClientRequestHandler(Socket clientSocket) {
			try {
				this.clientSocket = clientSocket;
				clientIn = new ObjectInputStream(clientSocket.getInputStream());
				clientOut = new ObjectOutputStream(clientSocket.getOutputStream());
				
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
					ArrayList<String> hosts = (ArrayList<String>) object;
					updateHosts(hosts);
				}

			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} finally {
				try {
					clientIn.close();
					clientOut.close();
					clientSocket.close();
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		private void inputHandler(String input) {
			Matcher matcher = Pattern.compile(
					"(\\w+)").matcher(input);

			if (!matcher.find()) {
				String message = "-linda: invalid command";
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
						clientOut.writeObject(addHostCommand(input));
						break;
					case "delete":
						clientOut.writeObject(deleteHostCommand(input));
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
					case "get_hosts":
						getHostsCommand();
						break;
					case "remove_tuple":
						getTupleCommand(input, true);
						break;
					case "get_tuple":
						getTupleCommand(input, false);
						break;
					case "contains_tuple":
						containsTupleCommand(input);
						break;
					default:
						String message = "-linda: invalid command";
						clientOut.writeObject(message);
						break;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

		private String addHostCommand(String command) {
			String[] hosts = LindaInputParser.getHosts(command);

			if (hosts.length == 0) {
				return "-linda: invalid command arguments";
			}

			ArrayList<Host> hostsToAdd = new ArrayList<Host>();

			// loop through all hosts in the add hosts command
			for (int i = 0; i < hosts.length; i++) {
				Host h = Host.readHost(hosts[i]);
				
				Socket socket = null;
				ObjectOutputStream oos = null;
				ObjectInputStream ois = null;
				try {
					// get the connected hosts of those added hosts
					socket = new Socket(h.getIPAddress(), h.getPort());
					oos = new ObjectOutputStream(socket.getOutputStream());
					ois = new ObjectInputStream(socket.getInputStream());
					
					oos.writeObject(new String("get_hosts"));
					Object o = ois.readObject();
					
					if (o instanceof ArrayList) {
						@SuppressWarnings("unchecked")
						ArrayList<String> retrievedHosts = (ArrayList<String>) o;
						for (int j = 0; j < retrievedHosts.size(); j ++) {
							Host retrievedHost = Host.readHost(retrievedHosts.get(j));
							synchronized (connectedHosts) {
								if (!hostsToAdd.contains(retrievedHost) && !connectedHosts.contains(retrievedHost)) {
									hostsToAdd.add(retrievedHost);
								}
							}
						}
					}
					
				} catch (IOException e) {
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				} finally {
					try {
						if (ois != null) {
							ois.close();
						}
						if (oos != null) {
							oos.close();
						}
						if (socket != null) {
							socket.close();
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
			
			addHosts(hostsToAdd.toArray(new Host[hostsToAdd.size()]));
			return "";
		}
		
		private String deleteHostCommand(String command) {
			String[] hosts = LindaInputParser.getHostNames(command);

			if (hosts.length == 0) {
				return "-linda: invalid command arguments";
			}

			Host[] hostsToDelete = new Host[hosts.length];

			for (int i = 0; i < hosts.length; i++) {
				hostsToDelete[i] = new Host(hosts[i]);
			}
			
			deleteHosts(hostsToDelete);
			return "";
		}
		
		private void getHostsCommand() {
			try {
				synchronized (connectedHosts) {
					clientOut.writeObject(buildHostStrings(connectedHosts));
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		private void inCommand(String command) {
			getTupleCommand(command, true);
		}

		private void rdCommand(String command) {
			getTupleCommand(command, false);
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
						putTuple(tuple);
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
							ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
							
							oos.writeObject(new String("tuple"));
							ois.close();
							oos.close();
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

					ois.close();
					oos.close();
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
		private void containsTupleCommand(String command) {
			Tuple tuple = new Tuple(LindaInputParser.parseTupleQuery(command));
			Tuple containedTuple = containsTuple(tuple);
			
			String message = (containedTuple != null) ? "(" + containedTuple.toString() + ")" : "";
			try {
				clientOut.writeObject(message);
				Object o = clientIn.readObject();
		
				if ((o instanceof String) && ((String) o).equals(ACK) && containedTuple != null) {
					deleteTuple(containedTuple);
				}

			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		
		/*
		 * gets a tuple from the tuple store, second argument determines whether the tuple
		 * must be removed or not
		 */
		private void getTupleCommand(String command, boolean removeTuple) {
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
							while (containsTuple(tuple) == null) {
								tupleLock.wait();
							}

							if (removeTuple) {
								deleteTuple(tuple);
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

						ois.close();
						oos.close();
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

				try {
				synchronized(tupleLock) {
					// test if the tuple is stored locally
					while (result == null) {
						result = containsTuple(tuple);
						if (removeTuple && result != null) {
							deleteTuple(result);
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

								ois.close();
								oos.close();
								socket.close();

								if (o instanceof String) {
									message = (String) o;
								
									if (message.length() > 0) {
										ipAddr = h.getIPAddress();
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
