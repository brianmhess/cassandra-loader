package hess.loader.parser;

import java.lang.String;
import java.util.UUID;
import java.lang.IllegalArgumentException;

// UUID parser
public class UUIDParser implements Parser {
    public UUID parse(String toparse) throws IllegalArgumentException {
	if (null == toparse)
	    return null;
	return UUID.fromString(toparse);
    }
}
