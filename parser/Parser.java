package hess.loader.parser;

import java.text.ParseException;

// Parsing Interface - one method parse(String)
public interface Parser {
    public Object parse(String toparse) throws ParseException;
}
