package hess.loader.parser;

import java.lang.String;
import java.math.BigInteger;
import java.lang.NumberFormatException;

// BigInteger parser
public class BigIntegerParser implements Parser {
    public BigInteger parse(String toparse) throws NumberFormatException {
	if (null == toparse)
	    return null;
	return new BigInteger(toparse);
    }
}
