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

import java.util.Date;
import java.text.ParseException;

import com.datastax.driver.core.LocalDate;

public class LocalDateParser extends AbstractParser {
    private DateParser dateParser;

    public LocalDateParser(String inFormatString) {
        dateParser = new DateParser(inFormatString);
    }

    public LocalDate parseIt(String toparse) throws ParseException {
        if (null == toparse)
            return null;
        Date d = dateParser.parseIt(toparse);
        LocalDate ret = LocalDate.fromMillisSinceEpoch(d.getTime());
        return ret;
    }

    public String format(Object o) {
        LocalDate v = (LocalDate)o;
        if (v == null)
            return null;
        Date d = new Date(v.getMillisSinceEpoch());
        return dateParser.format(d);
    }
}
