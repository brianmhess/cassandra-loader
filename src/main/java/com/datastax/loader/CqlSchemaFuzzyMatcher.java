package com.datastax.loader;

import me.xdrop.fuzzywuzzy.FuzzySearch;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by sebastianestevez on 12/6/16.
 */

public class CqlSchemaFuzzyMatcher {
    public CqlSchemaFuzzyMatcher() {
    }

    public String match(String keyspace, String table, BufferedReader reader, String delimiter, CqlDelimParser cdp) {
        String schemaString = keyspace.replaceAll("\"","")+ "." + table.replaceAll("\"","") + "(";
        List<String> columnNames = cdp.getColumnNames();
        try {
            String header = reader.readLine();
            //this didn' work
            //System.out.println(header);
            //List<Object> headerList = cdp.parse(header);
            if (delimiter == null){
                delimiter = ",";
            }
            List<String> headerList = Arrays.asList(header.split(delimiter));
            String[] resultColumns = new String[headerList.size()];
            if (headerList.size()> columnNames.size()){
                System.err.println("There are too many columns in the header.");
                System.exit(1);
            }
            int i =0;
            for (Iterator<String> headerIterator = headerList.iterator(); headerIterator.hasNext();){
                int currentRatio = 0;
                String currentColumn = "";
                String headerField = (String)headerIterator.next();
                for (Iterator<String> columnIterator = columnNames.iterator(); columnIterator.hasNext(); ) {
                    String column= columnIterator.next().replaceAll("\"","");
                    int newRatio = FuzzySearch.ratio(headerField,column);
                    if (newRatio > currentRatio) {
                        currentColumn = column;
                        currentRatio = newRatio;
                    }
                }
                if (currentRatio < 100){
                    System.err.println("The match between header "+ headerField + " and column " +  currentColumn + " was not exact.");
                    System.err.println("Proceeding with fuzzy match of score: " + currentRatio);
                }
                resultColumns[i]=currentColumn;
                i++;
                columnNames.remove(columnNames.contains(currentColumn));
            }
            schemaString = schemaString + String.join(",", resultColumns) + ")";
        } catch (IOException e) {
            e.printStackTrace();
        }
        return schemaString;
    }
}
