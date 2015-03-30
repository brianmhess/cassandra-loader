package hess.loader.parser;

import java.lang.String;
import java.lang.Integer;
import java.lang.Number;
import java.util.Locale;
import java.text.ParseException;

// Integer parser - use the Number parser
public class IntegerParser extends NumberParser {
    public IntegerParser() {
	super();
    }
    
    public IntegerParser(Locale inLocale) {
	super(inLocale);
    }
    
    public Integer parse(String toparse) throws ParseException {
	Number val = super.parse(toparse);
	return (null == val) ? null : val.intValue();
    }
}
