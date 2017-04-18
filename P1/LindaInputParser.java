import java.io.*;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LindaInputParser {
	private static final String ADD_HOST_REGEX =
		"\\(\\s*(\\w+)\\s*,\\s*((\\d{1,3}.){3}\\d{1,3})\\s*,\\s*(\\d+)\\s*\\)";
	private static final String TUPLE_REGEX = "(\\d+\\.\\d+|\\d+|\".+\")";
	private static final String TUPLE_QUERY_REGEX =
		"(\\d+\\.\\d+|\\d+|\".+\")|\\?\\w+:(int|string|float)";

	public static String[] getHosts(String command) {
		ArrayList<String> hosts = new ArrayList<String>();
		
		Matcher matcher = Pattern.compile(ADD_HOST_REGEX).matcher(command);
		
		while (matcher.find()) {
			hosts.add(matcher.group(1) + "," + matcher.group(2) +
					"," + matcher.group(4));
		}

		return hosts.toArray(new String[hosts.size()]);
	}

	public static String[] parseTuple(String command) {
		int index = command.indexOf("(");
		if (index == -1 || command.indexOf(")") == -1) {
			return new String[0];
		}

		String tuple = command.substring(index);
		ArrayList<String> tokens = new ArrayList<String>();
		Matcher matcher = Pattern.compile(TUPLE_REGEX).matcher(tuple);

		while (matcher.find()) {
			tokens.add(matcher.group(1));
		}

		return tokens.toArray(new String[tokens.size()]);
	}

	public static String[] parseTupleQuery(String command) {
		int index = command.indexOf("(");
		if (index == -1 || command.indexOf(")") == -1) {
			return new String[0];
		}

		String tuple = command.substring(index);
		ArrayList<String> tokens = new ArrayList<String>();
		Matcher matcher = Pattern.compile(TUPLE_QUERY_REGEX).matcher(tuple);

		while (matcher.find()) {
			tokens.add(matcher.group());
		}

		return tokens.toArray(new String[tokens.size()]);

	}
}
