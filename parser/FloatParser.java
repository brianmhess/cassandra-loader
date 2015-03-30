package hess.loader.parser;

import java.lang.String;
import java.lang.Float;
import java.lang.Number;
import java.util.Locale;
import java.text.ParseException;

// Float parser - use the Number parser
public class FloatParser extends NumberParser {
    public FloatParser() {
	super();
    }
    
    public FloatParser(Locale inLocale) {
	super(inLocale);
    }
    
    public Float parse(String toparse) throws ParseException {
	Number val = super.parse(toparse);
	return (null == val) ? null : val.floatValue();
    }
}
