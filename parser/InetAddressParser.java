package hess.loader.parser;

import java.lang.String;
import java.net.InetAddress;
import java.text.ParseException;
import java.net.UnknownHostException;

// InetAddress parser
public class InetAddressParser implements Parser {
    public InetAddress parse(String toparse) throws ParseException {
	if (null == toparse)
	    return null;
	InetAddress ret;
	try {
	    ret = InetAddress.getByName(toparse);
	}
	catch (UnknownHostException uhe) {
	    throw new ParseException("Error parsing Inet: " + uhe.getMessage(), 0);
	}
	return ret;
    }
}
