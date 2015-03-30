package hess.loader.parser;

import java.lang.String;
import java.util.Date;
import java.util.Locale;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.ParseException;

// Date parser - takes a format string
public class DateParser implements Parser {
    private DateFormat format;
    public DateParser(String inFormatString) {
	if (null == inFormatString)
	    format = new SimpleDateFormat();
	else
	    format = new SimpleDateFormat(inFormatString, Locale.ENGLISH);
    }
    
    public Date parse(String toparse) throws ParseException {
	if (null == toparse)
	    return null;
	return format.parse(toparse);
    }
}
