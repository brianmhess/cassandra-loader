package hess.loader.parser;

import java.lang.String;
import java.math.BigDecimal;
import java.lang.NumberFormatException;

// BigDecimal parser
public class BigDecimalParser implements Parser {
    public BigDecimal parse(String toparse) throws NumberFormatException {
	if (null == toparse)
	    return null;
	return new BigDecimal(toparse);
    }
}
