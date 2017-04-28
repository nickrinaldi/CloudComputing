import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*
 * Server class that starts the host server and the client side prompt
 */
public class Server {
	private static final String NOTIFY_HOSTS_COMMAND = "tuple";
	private static final String PUT_TUPLE_COMMAND = "out";
	private static final String READ_TUPLE_COMMAND = "rd";
	private static final String DELETE_TUPLE_COMMAND = "in";
	private static final String ADD_HOSTS_COMMAND = "add";
	private static final String DELETE_HOSTS_COMMAND = "delete";
	private static final String GET_HOSTS = "get_hosts";
	private static final String REMOVE_TUPLE = "remove_tuple";
	private static final String GET_TUPLE = "get_tuple";
	private static final String PUT_TUPLE = "put_tuple";
	private static final String CONTAINS_TUPLE = "contains_tuple";
	private static final String BACKUP_TUPLES = "backup_tuples";
	private static final String GET_BACKUP_TUPLES = "get_backup_tuples";
	private static final String STORE_ON_BACKUP = "store_on_backup";
	
//	private static final String DIRECTORY_PREFIX =
//			"C:/Users/Nick/Documents/GitHub/CloudComputing/P2/";
	private static final String DIRECTORY_PREFIX =
			"C:/Users/User/Documents/GitHub/CloudComputing/P2/";
	
	private Host host;
	private ArrayList<Host> connectedHosts;
	private ArrayList<Tuple> tuples;
	private ArrayList<Tuple> backupTuples;
	// tupleLock to prevent race conditions for client handlers
	private Object tupleLock = new Object();
	// hostLock to prevent race conditions for client handlers
	//private Object hostLock = new Object();
	
	String filePath;

	public Server() {
	}

	public Server(String name) {
		this.host = new Host(name);
		this.connectedHosts = new ArrayList<Host>();
		this.tuples = new ArrayList<Tuple>();
		this.backupTuples = new ArrayList<Tuple>();
		
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

	private void startupFromCrash() {
		// startup after crash, has connected hosts
		synchronized (connectedHosts) {
			int index = connectedHosts.indexOf(host);
			if (index >= 0 && index < connectedHosts.size()) {
				connectedHosts.get(index).setAddress(host.getAddress());
				
				for (int i = 0; i < connectedHosts.size(); i++) {
					Host connectedHost = connectedHosts.get(i);
					
					if (host.equals(connectedHost)) {
						// don't communicate if connected host is the same host
						continue;
					}

					try {
						int connectedHostPort = connectedHost.getPort();
						InetAddress connectedHostAddress = connectedHost.getIPAddress();

						Socket socket = new Socket(connectedHostAddress, connectedHostPort);

						ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
						ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
						
						out.writeObject(GET_HOSTS);
						
						Object o = in.readObject();
						if (o instanceof ArrayList) {
							@SuppressWarnings("unchecked")
							ArrayList<String> hosts = (ArrayList<String>) o;
							updateHosts(hosts);
						}
						
						out.close();
						in.close();
						socket.close();
						break;

					} catch (Exception e) {
						System.out.println("error getting connected host information from: " + connectedHost.getName());
					}
				}
				
				writeHosts();
				broadcastObject(buildHostStrings(connectedHosts));
			}
		}
		
		// get tuples
		synchronized (tuples) {
			ArrayList<Host> hosts = new ArrayList<Host>(connectedHosts);
			Collections.sort(hosts, new HostRangeComparator());
			
			Host backup = hosts.get(hosts.size() / 2);
			
			try {
				int connectedHostPort = backup.getPort();
				InetAddress connectedHostAddress = backup.getIPAddress();

				Socket socket = new Socket(connectedHostAddress, connectedHostPort);

				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
				ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
				
				out.writeObject(GET_BACKUP_TUPLES);
				
				Object o = in.readObject();
				if (o instanceof ArrayList) {
					@SuppressWarnings("unchecked")
					ArrayList<String> tupleStrings = (ArrayList<String>) o;
					for (String tupleString : tupleStrings) {
						tuples.add(new Tuple(LindaInputParser.parseTuple(tupleString)));
					}
				}
				
				out.close();
				in.close();
				socket.close();

			} catch (Exception e) {
				System.out.println("error getting my tuples which should be backed by host: " + backup.getName());
			}
			
			writeTuples();
		}
	}
	
	/*
	 * add hosts method that communicated with the other hosts to be added, the hosts
	 * in the parameter are unique and not already connected
	 */
	private void addHosts(Host[] hosts) {
		
		boolean addedHost = false;
		for (int i = 0; i < hosts.length; i++) {
			if (!hosts[i].equals(host)) {
				addedHost = true;
				addHost(hosts[i]);
			}
		}
		
		if (addedHost) {
			synchronized (connectedHosts) {
				writeHosts();
				broadcastObject(buildHostStrings(connectedHosts));
				redistributeTuples();
			}
		}
	}

	private void addHost(Host addedHost) {
		synchronized (connectedHosts) {
			int maxRange = 0;
			int startIndex = 0;
			int endIndex = 0;
			Host splittedHost = null;
			
			for (int i = 0; i < connectedHosts.size(); i++) {
				Host connectedHost = connectedHosts.get(i);
				if (maxRange < connectedHost.getRange()) {
					maxRange = connectedHost.getRange();
					startIndex = connectedHost.getMin() + maxRange / 2;
					endIndex = connectedHost.getMax();
					splittedHost = connectedHost;
				}
			}
			
			connectedHosts.add(addedHost);
			
			if (splittedHost == null) {
				// error
				return;
			}
			
			addedHost.setMin(startIndex);
			addedHost.setMax(endIndex);
			splittedHost.setMax(startIndex - 1);
		}
	}
	
	/*
	 * delete the hosts and redistribute the tuples
	 */
	private void deleteHosts(Host[] hosts) {
		boolean removeSelf = false;
		
		ArrayList<Host> oldConnectedHosts = new ArrayList<Host>(connectedHosts);
		
		for (int i = 0; i < hosts.length; i++) {
			if (hosts[i].equals(host)) {
				removeSelf = true;
			}
			
			deleteHost(hosts[i]);
		}
		
		synchronized (connectedHosts) {
			ArrayList<Host> newConnectedHosts = connectedHosts;
			connectedHosts = oldConnectedHosts;
			broadcastObject(buildHostStrings(newConnectedHosts));
			connectedHosts = newConnectedHosts;
			redistributeTuples();
		}
		
		if (removeSelf) {
			connectedHosts.clear();
			host.setMin(0);
			host.setMax((int) Math.pow(2, 16) - 1);
			connectedHosts.add(host);
		}
		
		writeHosts();
	}

	private void deleteHost(Host deletedHost) {
		synchronized (connectedHosts) {
			
			Host hostToInheritTuples = null;
			Host hostToDelete = null;
			
			for (int i = 0; i < connectedHosts.size(); i++) {
				Host connectedHost = connectedHosts.get(i);
				if (connectedHost.equals(deletedHost)) {
					hostToDelete = connectedHost;
					break;
				}
			}
			
			if (hostToDelete == null) {
				return;
			}
			
			for (int i = 0; i < connectedHosts.size(); i++) {
				hostToInheritTuples = connectedHosts.get(i);
				
				if (hostToInheritTuples.getMax() == hostToDelete.getMin() - 1) {
					break;
				}
			}
			
			connectedHosts.remove(hostToDelete);
			if (hostToInheritTuples == null) {
				return;
			}
			
			hostToInheritTuples.setMax(hostToDelete.getMax());
		}
	}
	
	private Host getBackupHost(Host h) {
		ArrayList<Host> hosts = new ArrayList<Host>(connectedHosts);
		Collections.sort(hosts, new HostRangeComparator());
		
		int half = hosts.size() / 2;
		int current = hosts.indexOf(h);
		Host backup = hosts.get((current + half) % hosts.size());
		return backup;
	}
	
	/*
	 * tells the backup host to backup our tuples
	 */
	private void backupTuples() {
		Host backup = getBackupHost(host);
		
		ArrayList<String> tupleStrings = new ArrayList<String>();
		for (Tuple tuple : tuples) {
			tupleStrings.add("(" + tuple.toString() + ")");
		}
		
		try {
			int connectedHostPort = backup.getPort();
			InetAddress connectedHostAddress = backup.getIPAddress();

			Socket socket = new Socket(connectedHostAddress, connectedHostPort);

			ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
			ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
			
			out.writeObject(BACKUP_TUPLES);
			out.writeObject(tupleStrings);
			
			out.close();
			in.close();
			socket.close();

		} catch (Exception e) {
			System.out.println("error giving backups to host " + backup.getName());
		}
	}
	
	/*
	 * put tuple in the tuple store
	 */
	private void putTuple(Tuple tuple) {
		synchronized (tuples) {
			tuples.add(tuple);
			writeTuples();
			backupTuples();
		}
	}
	
	/*
	 * delete tuple in the tuple store
	 */
	private void deleteTuple(Tuple tuple) {
		synchronized (tuples) {
			tuples.remove(tuple);
			writeTuples();
			backupTuples();
		}
	}
	
	private void storeOnBackup(Tuple tuple) {
		synchronized (backupTuples) {
			backupTuples.add(tuple);
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
				System.out.println("error broadcasting to host: " + connectedHost.getName());
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
				Host connectedHost = Host.readHostWithRange(hosts.get(i));
				
				// must change self address in case the incoming host list is from another host
				if (connectedHost.equals(host)) {
					connectedHost.setAddress(host.getAddress());
					host.setMin(connectedHost.getMin());
					host.setMax(connectedHost.getMax());
				}
				connectedHosts.add(connectedHost);
			}
			
			// if self is not contained in the list
			if (!connectedHosts.contains(host)) {
				connectedHosts.clear();
				host.setMin(0);
				host.setMax((int) Math.pow(2, 16) - 1);
				connectedHosts.add(host);
			}
	
			writeHosts();
			redistributeTuples();
		}
	}

	/*
	 * method to recalculate the hashes and distribute tuples accordingly
	 */
	private void redistributeTuples() {
		synchronized (tuples) {
			Iterator<Tuple> iterator = tuples.iterator();
			while (iterator.hasNext()) {
				Tuple tuple = iterator.next();
				int hash = tuple.hashCode();
				for (Host connectedHost : connectedHosts) {
					if (!connectedHost.equals(host) && hash >= connectedHost.getMin() &&
							hash <= connectedHost.getMax()) {
						
						// move the tuple
						try {
							int connectedHostPort = connectedHost.getPort();
							InetAddress connectedHostAddress = connectedHost.getIPAddress();

							Socket socket = new Socket(connectedHostAddress, connectedHostPort);

							ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
							ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
							
							out.writeObject(PUT_TUPLE_COMMAND + "(" + tuple.toString() + ")");
							in.close();
							out.close();
							socket.close();
							
							iterator.remove();

						} catch (Exception e) {
							System.out.println("error rehashing tuple to host: " + connectedHost.getName());
						}
					}
				}
			}
			writeTuples();
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
				connectedHosts.add(Host.readHostWithRange(line));
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
			
			bw.write("tuples:");
			bw.newLine();
			for (int i = 0; i < tuples.size(); i++) {
				bw.write(tuples.get(i).toString());
				bw.newLine();
			}

			bw.write("backup_tuples:");
			bw.newLine();
			for (int i = 0; i < backupTuples.size(); i++) {
				bw.write(backupTuples.get(i).toString());
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
				startupFromCrash();
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
				System.out.println("error connected the input and output stream");
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
					System.out.println("error closing the input stream, output stream, and socket");
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
					System.out.println("error writing to the client output stream");
				}

				return;
			}

			// evaluate the command
			try {
				switch(matcher.group(1)) {
					case NOTIFY_HOSTS_COMMAND:
						synchronized(tupleLock) {
							tupleLock.notifyAll();
						}
						break;
					case ADD_HOSTS_COMMAND:
						clientOut.writeObject(addHostCommand(input));
						break;
					case DELETE_HOSTS_COMMAND:
						clientOut.writeObject(deleteHostCommand(input));
						break;
					case DELETE_TUPLE_COMMAND:
						inCommand(input);
						break;
					case PUT_TUPLE_COMMAND:
						outCommand(input);
						break;
					case READ_TUPLE_COMMAND:
						rdCommand(input);
						break;
					case GET_HOSTS:
						getHostsCommand();
						break;
					case REMOVE_TUPLE:
						getTupleCommand(input, true);
						break;
					case GET_TUPLE:
						getTupleCommand(input, false);
						break;
					case PUT_TUPLE:
						putTuple(new Tuple(LindaInputParser.parseTuple(input)));
						break;
					case CONTAINS_TUPLE:
						containsTupleCommand(input);
						break;
					case BACKUP_TUPLES:
						backupTuplesCommand();
						break;
					case GET_BACKUP_TUPLES:
						getBackupTuples();
						break;
					case STORE_ON_BACKUP:
						storeOnBackup(new Tuple(LindaInputParser.parseTuple(input)));
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

		private void backupTuplesCommand() {
			synchronized (backupTuples) {
				try {
					Object o = clientIn.readObject();
					
					if (o instanceof ArrayList) {
						@SuppressWarnings("unchecked")
						ArrayList<String> tupleStrings = (ArrayList<String>) o;
						backupTuples.clear();
						for (String tupleString : tupleStrings) {
							Tuple tuple = new Tuple(LindaInputParser.parseTuple(tupleString));
							backupTuples.add(tuple);
						}
					}
					
				} catch (Exception e) {
					System.out.println("error storing the backups");
				}
				synchronized (tuples) {
					writeTuples();
				}
			}
		}
		
		private void getBackupTuples() {
			synchronized (backupTuples) {
				ArrayList<String> backupTupleStrings = new ArrayList<String>();
				for (Tuple tuple : backupTuples) {
					String tupleStr = "(" + tuple.toString() + ")";
					backupTupleStrings.add(tupleStr);
				}
			
				try {
					clientOut.writeObject(backupTupleStrings);
				} catch (IOException e) {
					System.out.println("error writing the backup tuples to the output stream");
				}
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
							Host retrievedHost = Host.readHostWithRange(retrievedHosts.get(j));
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
				System.out.println("error writing hosts to the client output stream");
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
		
			int hash = tuple.hashCode();
			Host h = null;
			Host backup = null;
			for (Host connectedHost : connectedHosts) {
				if (hash >= connectedHost.getMin() && hash <= connectedHost.getMax()) {
					h = connectedHost;
					backup = getBackupHost(h);
					break;
				}
			}
			
			if (h == null) {
				// error
				return;
			}

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
							System.out.println("error notifying host: " + connectedHosts.get(i).getName() + " of a stored tuple");
						}
					}

				}

				else {
					try {
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
						
					} catch (IOException e) {
						// can't communicate with host, store the tuple on the backup
						try {
							Socket socket = new Socket(backup.getIPAddress(), backup.getPort());
							ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
							ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
		
							oos.writeObject(STORE_ON_BACKUP + "(" + tuple.toString() + ")");
		
							ois.close();
							oos.close();
							socket.close();
							
						} catch (IOException err) {
							System.out.println("error communicating with the backup host");
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

			try {
				clientOut.writeObject(message);
			} catch (Exception e) {
				System.out.println("error writing the message to the client output stream");
			}
		}

		/*
		 * contains tuple module for servers to communicate between each other
		 */
		private void containsTupleCommand(String command) {
			synchronized (tupleLock) {
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
					System.out.println("error checking if tuple is contained");
				}
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
				int hash = tuple.hashCode();
				Host h = null;
				for (Host connectedHost : connectedHosts) {
					if (hash >= connectedHost.getMin() && hash <= connectedHost.getMax()) {
						h = connectedHost;
						break;
					}
				}
				
				if (h == null) {
					// error
					return;
				}

				try {
					InetAddress address = h.getIPAddress();
					int port = h.getPort();

					if (host.equals(h)) {
						Tuple result = new Tuple();
						synchronized(tupleLock) {
							while ((result = containsTuple(tuple)) == null) {
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
