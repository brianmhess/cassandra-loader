/*
 * Copyright 2015 Brian Hess
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.loader.parser;

import java.io.StringReader;
import java.io.IOException;
import java.text.ParseException;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import org.apache.commons.lang3.StringEscapeUtils;

public abstract class AbstractParser<E> implements Parser<E> {
    public abstract E parseIt(String toparse) throws ParseException;

    public String format(Row row, int index) throws IndexOutOfBoundsException, InvalidTypeException {
        if (row.isNull(index))
            return null;
        return format(row.getObject(index));
    }

    public abstract String format(Object o);

    public E parse(String toparse) throws ParseException {
        String toparseit = unquote(toparse);
        return parseIt(toparseit);
    }

    public static String quote(String instr) {
        if (null == instr)
            return null;
        return "\"" + escape(instr) + "\"";
    }

    public static String unquote(String instr) {
        if (null == instr)
            return null;
        if ((instr.startsWith("\"")) && (instr.endsWith("\"")))
            return unescape(instr.substring(1, instr.length() - 1));
        return instr;
    }

    public static String unstripQuote(String instr) {
        if (null == instr)
            return null;
        return "\"" + instr + "\"";
    }

    public static String stripQuote(String instr) {
        if (null == instr)
            return null;
        if ((instr.startsWith("\"")) && (instr.endsWith("\"")))
            return instr.substring(1, instr.length() - 1);
        return instr;
    }

    public static String escape(String instr) {
        if (null == instr)
            return null;
        return StringEscapeUtils.escapeJava(instr);
    }

    public static String unescape(String instr) {
        if (null == instr)
            return null;
        return StringEscapeUtils.unescapeJava(instr);
    }
}
