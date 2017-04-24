import java.net.*;

/*
 * Host class representing a name plus ip address and port
 * A server is equal to another if there is a name conflict
 * or if they have the same ip/port combination
 */
public class Host {
	private String name;
	private InetSocketAddress serverAddress;

	public Host() {
	}

	public Host(String name) {
		this.name = name;
		serverAddress = null;
	}

	public static Host readHost(String hostStr) {
		String[] tokens = hostStr.split(",");

		if (tokens.length != 3) {
			return null;
		}

		Host result = new Host(tokens[0]);

		try {
		result.serverAddress = new InetSocketAddress(InetAddress.getByName(tokens[1]),
				Integer.parseInt(tokens[2]));
		} catch (UnknownHostException e) {
			e.printStackTrace();
			result = null;
		}

		return result;
	}

	public String getName() {
		return name;
	}

	public InetSocketAddress getAddress() {
		return serverAddress;
	}

	public void setAddress(InetSocketAddress address) {
		serverAddress = new InetSocketAddress(address.getAddress(), address.getPort());
	}

	public InetAddress getIPAddress() {
		return (serverAddress == null) ? null : serverAddress.getAddress();
	}

	public int getPort() {
		return (serverAddress == null) ? -1 : serverAddress.getPort();
	}

	@Override
	public String toString() {
		String result = name;

		if (serverAddress != null) {
			result += ("," + serverAddress.getAddress().getHostAddress() +
					"," + serverAddress.getPort());
		}

		return result;
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof Host)) {
			return false;
		}

		Host host = (Host) o;

		if (name.equals(host.name)) {
			return true;
		}

		if (serverAddress != null && host.serverAddress != null) {
			return serverAddress.equals(host.serverAddress);
			
			// returning false for testing purposes on same machine
			// uncomment top line and comment following line for submission
			//return false;
		}

		return false;
	}
}
