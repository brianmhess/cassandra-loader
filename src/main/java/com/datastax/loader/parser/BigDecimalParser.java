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

import java.lang.String;
import java.math.BigDecimal;
import java.lang.NumberFormatException;
import java.lang.IndexOutOfBoundsException;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidTypeException;

// BigDecimal parser
public class BigDecimalParser extends AbstractParser {
    public BigDecimal parse(String toparse) throws NumberFormatException {
	if (null == toparse)
	    return null;
	return new BigDecimal(toparse);
    }

    public String format(Row row, int index) throws IndexOutOfBoundsException, InvalidTypeException {
	return row.isNull(index) ? null : row.getDecimal(index).toString();
    }
}
