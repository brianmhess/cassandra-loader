package hess.loader.parser;

import java.lang.String;
import java.lang.Number;
import java.util.Locale;
import java.text.NumberFormat;
import java.text.ParseException;

// General number parser
// This is useful as it can take care of Locales for us
// That means comma as a decimal separator, etc.
public class NumberParser implements Parser {
    protected NumberFormat nf;
    public NumberParser() {
	this(null);
    }
    
    public NumberParser(Locale locale) {
	if (null == locale)
	    locale = Locale.ENGLISH;
	nf = NumberFormat.getInstance(locale);
    }
    
    // Need this method for the subclasses
    public Number parse(String toparse) throws ParseException {
	if ((null == toparse) || (0 == toparse.length()))
	    return null;
	return nf.parse(toparse);
    }
}
