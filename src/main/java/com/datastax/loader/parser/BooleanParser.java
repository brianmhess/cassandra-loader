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
import java.lang.Boolean;
import java.text.ParseException;
import java.lang.IndexOutOfBoundsException;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidTypeException;

// Boolean parser - handles any way that Booleans can be expressed in Java
public class BooleanParser implements Parser {
    public static enum BoolStyle { BoolStyle_TrueFalse, 
	    BoolStyle_10, 
	    BoolStyle_TF, 
	    BoolStyle_YN, 
	    BoolStyle_YesNo }
    
    private String boolTrue;
    private String boolFalse;
    private static String BOOLSTYLE_1_0 = "1_0";
    private static String BOOLSTYLE_T_F = "T_F";
    private static String BOOLSTYLE_Y_N = "Y_N";
    private static String BOOLSTYLE_YES_NO = "YES_NO";
    private static String BOOLSTYLE_TRUE_FALSE = "TRUE_FALSE";
    
    public BooleanParser() {
	this(BoolStyle.BoolStyle_TrueFalse);
    }
    
    public BooleanParser(BoolStyle inBoolStyle) {
	boolTrue = "TRUE";
	boolFalse = "FALSE";
	if (null == inBoolStyle)
	    inBoolStyle = BoolStyle.BoolStyle_TrueFalse;
	switch (inBoolStyle) {
	case BoolStyle_10:
	    boolTrue = "1";
	    boolFalse = "0";
	    break;
	case BoolStyle_TF:
	    boolTrue = "T";
	    boolFalse = "F";
	    break;
	case BoolStyle_YN:
	    boolTrue = "Y";
	    boolFalse = "N";
	    break;
	case BoolStyle_YesNo:
	    boolTrue = "YES";
	    boolFalse = "NO";
	    break;
	default:
	    boolTrue = "TRUE";
	    boolFalse = "FALSE";
	    break;
	}
    }

    public BooleanParser(String inBoolTrue, String inBoolFalse) {
	boolTrue = inBoolTrue;
	boolFalse = inBoolFalse;		
    }

    public static BoolStyle getBoolStyle(String instr) {
	if (BOOLSTYLE_1_0.equalsIgnoreCase(instr))
	    return BoolStyle.BoolStyle_10;
	if (BOOLSTYLE_T_F.equalsIgnoreCase(instr))
	    return BoolStyle.BoolStyle_TF;
	if (BOOLSTYLE_Y_N.equalsIgnoreCase(instr))
	    return BoolStyle.BoolStyle_YN;
	if (BOOLSTYLE_YES_NO.equalsIgnoreCase(instr))
	    return BoolStyle.BoolStyle_YesNo;
	if (BOOLSTYLE_TRUE_FALSE.equalsIgnoreCase(instr))
	    return BoolStyle.BoolStyle_TrueFalse;
	return null;
    }

    public static String getOptions() {
	String ret = "'" + BOOLSTYLE_1_0 + "'";
	ret = ret + ", '" + BOOLSTYLE_T_F + "'";
	ret = ret + ", '" + BOOLSTYLE_Y_N + "'";
	ret = ret + ", '" + BOOLSTYLE_TRUE_FALSE + "'";
	ret = ret + ", '" + BOOLSTYLE_YES_NO + "'";
	return ret;
    };

    public Boolean parse(String toparse) throws ParseException {
	if (null == toparse)
	    return null;
	if (boolTrue.equalsIgnoreCase(toparse))
	    return new Boolean("TRUE");
	if (boolFalse.equalsIgnoreCase(toparse))
	    return new Boolean("FALSE");
	throw new ParseException("Boolean was not TRUE (" + boolTrue + ") or FALSE (" + boolFalse + ")", 0);
    }

    public String format(Row row, int index) throws IndexOutOfBoundsException, InvalidTypeException {
	if (row.isNull(index))
	    return null;
	boolean val = row.getBool(index);
	if (val)
	    return boolTrue;
	return boolFalse;
    }
}

