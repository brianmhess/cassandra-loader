package hess.loader.parser;

import java.lang.String;
import java.nio.ByteBuffer;

// ByteBuffer parser
public class ByteBufferParser implements Parser {
    public ByteBuffer parse(String toparse) {
	if (null == toparse)
	    return null;
	ByteBuffer bb = ByteBuffer.allocate(toparse.length());
	return bb.put(toparse.getBytes());
    }
}
