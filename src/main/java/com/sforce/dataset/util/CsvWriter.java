/*
 * Copyright (c) 2014, salesforce.com, inc.
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided 
 * that the following conditions are met:
 * 
 *    Redistributions of source code must retain the above copyright notice, this list of conditions and the 
 *    following disclaimer.
 *  
 *    Redistributions in binary form must reproduce the above copyright notice, this list of conditions and 
 *    the following disclaimer in the documentation and/or other materials provided with the distribution. 
 *    
 *    Neither the name of salesforce.com, inc. nor the names of its contributors may be used to endorse or 
 *    promote products derived from this software without specific prior written permission.
 *  
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED 
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A 
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR 
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED 
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) 
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING 
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
 * POSSIBILITY OF SUCH DAMAGE.
 */

package com.sforce.dataset.util;

import java.io.PrintWriter;
import java.io.Writer;
import java.util.List;


public class CsvWriter {
    private PrintWriter writer;
    private char delimiter = ',';
    private char quoteChar = '\"';

    public CsvWriter(Writer w, char delimiter, char quoteChar) {
        writer = new PrintWriter(w, true);
        if(delimiter != 0)
        {
        	this.delimiter = delimiter;
        }

        if(quoteChar != 0)
        {
        	this.quoteChar = quoteChar;
        }
    }

    public void writeRecord(String[] values) {
       	if(values != null && values.length!=0)
    	{ 
	        writeFirstField(values[0]);
	        for (int i=1; i<values.length; i++) {
	            writeField(values[i]);
	        }
	        endRecord();
    	}
    }

    public void writeRecord(List<String> values) {
    	if(values != null && !values.isEmpty())
    	{
    		writeFirstField(values.get(0));
	        for (int i=1; i<values.size(); i++) {
	            writeField(values.get(i));
	        }
	        endRecord();
    	}
    }

    public void close() {
        writer.close();
    }

    public void endRecord() {
        writer.println();
    }

    public void writeField(String value) {
        writer.print(this.delimiter);
        if (value != null && !value.isEmpty()) 
        {
        	writeFirstField(value);
        }
    }

    public void writeFirstField(String value) {
        if (value != null && !value.isEmpty()) 
        {
        	writer.print(encode(value, delimiter, quoteChar));
        }
    }
    		
    public static String encode(final String input, char delimiter, char quote) {
		
		final StringBuilder currentColumn = new StringBuilder();
		final String eolSymbols = "\n";
		final int lastCharIndex = input.length() - 1;
		
		boolean quotesRequiredForSpecialChar = false;
		
		boolean skipNewline = false;
		
		for( int i = 0; i <= lastCharIndex; i++ ) {
			
			final char c = input.charAt(i);
			
			if( skipNewline ) {
				skipNewline = false;
				if( c == '\n' ) {
					continue; // newline following a carriage return is skipped
				}
			}
			
			if( c == delimiter ) {
				quotesRequiredForSpecialChar = true;
				currentColumn.append(c);
			} else if( c == quote ) {
				quotesRequiredForSpecialChar = true;
				currentColumn.append(quote);
				currentColumn.append(quote);
			} else if( c == '\r' ) {
				quotesRequiredForSpecialChar = true;
				currentColumn.append(eolSymbols);
				skipNewline = true;
			} else if( c == '\n' ) {
				quotesRequiredForSpecialChar = true;
				currentColumn.append(eolSymbols);
			} else {
				currentColumn.append(c);
			}
		}
		
		final boolean quotesRequiredForMode = false;
		final boolean quotesRequiredForSurroundingSpaces = true 
			&& input.length() > 0 && (input.charAt(0) == ' ' || input.charAt(input.length() - 1) == ' ');
		
		if( quotesRequiredForSpecialChar || quotesRequiredForMode || quotesRequiredForSurroundingSpaces ) {
			currentColumn.insert(0, quote).append(quote);
		}
		
		return currentColumn.toString();
	}
}
