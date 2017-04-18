import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.security.MessageDigest;
import java.nio.ByteBuffer;
import java.io.Serializable;

public class Tuple implements Serializable {
	private static final String STRING_REGEX = "\".+\"";
	private static final String INT_REGEX = "\\d+";
	private static final String FLOAT_REGEX = "\\d+.\\d+";
	private static final String STRING_TYPE = "string";
	private static final String INT_TYPE = "int";
	private static final String FLOAT_TYPE = "float";
	private static final String FIELD_SEPARATOR = ", ";

	private String[] fields;

	public Tuple() {
		fields = new String[0];
	}

	public Tuple(String[] fields) {
		this.fields = new String[fields.length];

		for (int i = 0; i < fields.length; i++) {
			this.fields[i] = fields[i];
		}
	}

	public Tuple(String tupleStr) {
		this(tupleStr.split(FIELD_SEPARATOR));
	}

	public int size() {
		return fields.length;
	}

	public boolean containsQuery() {
		for (int i = 0; i < fields.length; i++) {
			if (fields[i] != null && fields[i].charAt(0) == '?') {
				return true;
			}
		}

		return false;
	}

	private boolean fieldMatch(String field1, String field2) {
		if (field1 == null || field2 == null) {
			// invalid fields return false
			return false;
		}
		
		if (field2.charAt(0) == '?') {
			String temp = field2;
			field2 = field1;
			field1 = temp;
		}
		
		if (field1.charAt(0) == '?') {
			// has query string
			String type = field1.split(":")[1];

			if (type == STRING_TYPE) {
				Matcher matcher = Pattern.compile(STRING_REGEX).matcher(field2);
					
				if (!matcher.find()) {
					return false;
				}	
			}
			else if (type == INT_TYPE) {
				Matcher matcher = Pattern.compile(INT_REGEX).matcher(field2);
					
				if (!matcher.find()) {
					return false;
				}	
			}
			else if (type == FLOAT_TYPE) {
				Matcher matcher = Pattern.compile(FLOAT_REGEX).matcher(field2);
					
				if (!matcher.find()) {
					return false;
				}	
			}
		}
		else if (!field1.equals(field2)) {
			return false;
		}
	
		return true;
	}

	@Override
	public String toString() {
		String result = "";
		
		for (int i = 0; i < fields.length; i++) {
			result += (fields[i] + FIELD_SEPARATOR);
		}

		if (result.length() >= 2) {
			result = result.substring(0, result.length() - 2);
		}

		return result;
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof Tuple)) {
			return false;
		}

		Tuple tuple = (Tuple) o;

		if (fields.length != tuple.fields.length) {
			return false;
		}

		for (int i = 0; i < fields.length && i < tuple.fields.length; i++) {
			if (!fieldMatch(fields[i], tuple.fields[i])) {
				return false;
			}
		}

		return true;
	}

	@Override
	public int hashCode() {
		String tupleStr = "";
		for (int i = 0; i < fields.length; i++) {
			String field = fields[i];

			if (field != null) {	
				if (field.contains("\"")) {
					field = field.substring(1, field.length() - 1);
				}
				tupleStr += field;
			}
		}

		try {
			MessageDigest md5 = MessageDigest.getInstance("MD5");
			byte[] hash = md5.digest(tupleStr.getBytes());
			int hashedInt = ByteBuffer.wrap(hash).getInt();
			if (hashedInt < 0) {
				hashedInt *= -1;
			}

			return hashedInt;

		} catch (Exception e) {
			System.err.println("alg not found");
			e.printStackTrace();
			return 0;
		}


	}

}
