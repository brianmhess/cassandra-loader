/*
 * Copyright 2019 Carl Livermore
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

import java.text.ParseException;
import java.time.format.DateTimeFormatter;
import java.time.LocalTime;

// Time parser - takes a format string
public class LocalTimeParser extends AbstractParser {
    private DateTimeFormatter dateTimeFormatter;

    public LocalTimeParser(String inFormatString) {
        if (null == inFormatString)
            dateTimeFormatter = DateTimeFormatter.ISO_LOCAL_TIME;
        else
            dateTimeFormatter = DateTimeFormatter.ofPattern(inFormatString);
    }

    public Long parseIt(String toparse) throws ParseException {
        if (null == toparse)
            return null;

        try {
            return LocalTime.parse(toparse, dateTimeFormatter).toNanoOfDay();
        } catch (Exception e) {
            throw new ParseException(e.getMessage(), 0);
        }
    }

    public String format(Object o) {
        LocalTime v = LocalTime.ofNanoOfDay((Long)o);
        return v.format(dateTimeFormatter);
    }
}
