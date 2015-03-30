package hess.loader.parser;

import java.lang.String;
import java.lang.Long;
import java.lang.Number;
import java.util.Locale;
import java.text.ParseException;

// Long parser - use the Number parser
public class LongParser extends NumberParser {
    public LongParser() {
	super();
    }
    
    public LongParser(Locale inLocale) {
	super(inLocale);
    }
    
    public Long parse(String toparse) throws ParseException {
	Number val = super.parse(toparse);
	return (null == val) ? null : val.longValue();
    }
}
