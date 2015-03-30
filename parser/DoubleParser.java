package hess.loader.parser;

import java.lang.String;
import java.lang.Double;
import java.lang.Number;
import java.util.Locale;
import java.text.ParseException;

// Double parser - use the Number parser
public class DoubleParser extends NumberParser {
    public DoubleParser() {
	super();
    }
    
    public DoubleParser(Locale inLocale) {
	super(inLocale);
    }
    
    public Double parse(String toparse) throws ParseException {
	Number val = super.parse(toparse);
	return (null == val) ? null : val.doubleValue();
    }
}
