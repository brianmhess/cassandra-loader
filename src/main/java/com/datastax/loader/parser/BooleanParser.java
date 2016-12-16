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

import java.text.ParseException;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidTypeException;

// Boolean parser - handles any way that Booleans can be expressed in Java
public class BooleanParser extends AbstractParser<Boolean> {
    public static enum BoolStyle { 
        BoolStyle_TrueFalse("TRUE_FALSE", "TRUE", "FALSE"), 
        BoolStyle_10("1_0", "1", "0"), 
        BoolStyle_TF("T_F", "T", "F"), 
        BoolStyle_YN("Y_N", "Y", "N"), 
        BoolStyle_YesNo("YES_NO", "YES", "NO");

        private String styleStr;
        private String trueStr;
        private String falseStr;

        BoolStyle(String inStyleStr, String inTrueStr, String inFalseStr) {
            styleStr = inStyleStr;
            trueStr = inTrueStr;
            falseStr = inFalseStr;
        }

        public String getStyle() {
            return styleStr;
        }

        public String getTrueStr() {
            return trueStr;
        }

        public String getFalseStr() {
            return falseStr;
        }
    }
    
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
        if (null == inBoolStyle)
            inBoolStyle = BoolStyle.BoolStyle_TrueFalse;
        boolTrue = inBoolStyle.getTrueStr();
        boolFalse = inBoolStyle.getFalseStr();
    }

    public BooleanParser(String inBoolTrue, String inBoolFalse) {
        boolTrue = inBoolTrue;
        boolFalse = inBoolFalse;                
    }

    public static BoolStyle getBoolStyle(String instr) {
        for (BoolStyle bs : BoolStyle.values()) {
            if (bs.getStyle().equalsIgnoreCase(instr)) {
                return bs;
            }
        }
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

    public Boolean parseIt(String toparse) throws ParseException {
        if (null == toparse)
            return null;
        if (boolTrue.equalsIgnoreCase(toparse))
            return new Boolean("TRUE");
        if (boolFalse.equalsIgnoreCase(toparse))
            return new Boolean("FALSE");
        throw new ParseException("Boolean was not TRUE (" + boolTrue + ") or FALSE (" + boolFalse + ")", 0);
    }

    public String format(Object o) {
        Boolean v = (Boolean)o;
        if (v)
            return boolTrue;
        return boolFalse;
    }
}

