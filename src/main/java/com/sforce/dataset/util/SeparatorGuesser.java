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

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SeparatorGuesser {
	
	
    static public class Separator {
        char separator;
        int totalCount = 0;
        int totalOfSquaredCount = 0;
        int currentLineCount = 0;
        
        double averagePerLine;
        double stddev;
    }

    static public char guessSeparator(File file, Charset inputFileCharset, boolean handleQuotes) {
        try {
            InputStream is = new FileInputStream(file);
            Reader reader = inputFileCharset != null ? new InputStreamReader(is, inputFileCharset) : new InputStreamReader(is);
            LineNumberReader lineNumberReader = new LineNumberReader(reader);

            try {
                List<Separator> separators = new ArrayList<SeparatorGuesser.Separator>();
                Map<Character, Separator> separatorMap = new HashMap<Character, SeparatorGuesser.Separator>();
                
                int totalChars = 0;
                int lineCount = 0;
                boolean inQuote = false;
                String s;
                while (totalChars < 64 * 1024 &&
                       lineCount < 100 &&
                       (s = lineNumberReader.readLine()) != null) {
                    
                    totalChars += s.length() + 1; // count the new line character
                    if (s.length() == 0) {
                        continue;
                    }
                    if (!inQuote) {
                        lineCount++;
                    }
                    
                    for (int i = 0; i < s.length(); i++) {
                        char c = s.charAt(i);
                        if ('"' == c) {
                            inQuote = !inQuote;
                        }
                        if (!Character.isLetterOrDigit(c) 
                                && !"\"' .-".contains(s.subSequence(i, i + 1)) 
                                && (!handleQuotes || !inQuote)) {
                            Separator separator = separatorMap.get(c);
                            if (separator == null) {
                                separator = new Separator();
                                separator.separator = c;
                                
                                separatorMap.put(c, separator);
                                separators.add(separator);
                            }
                            separator.currentLineCount++;
                        }
                    }
                    
                    if (!inQuote) {
                        for (Separator separator : separators) {
                            separator.totalCount += separator.currentLineCount;
                            separator.totalOfSquaredCount += separator.currentLineCount * separator.currentLineCount;
                            separator.currentLineCount = 0;
                        }
                    }
                }
                
                if (separators.size() > 0) {
                    for (Separator separator : separators) {
                        separator.averagePerLine = separator.totalCount / (double) lineCount;
                         separator.stddev = Math.sqrt(
                                 (((double)lineCount * separator.totalOfSquaredCount) - (separator.totalCount * separator.totalCount))
                                        / ((double)lineCount*(lineCount-1))
                            );
                    }
                    
                    Collections.sort(separators, new Comparator<Separator>() {
                        @Override
                        public int compare(Separator sep0, Separator sep1) {
                            return Double.compare(sep0.stddev / sep0.averagePerLine, 
                            sep1.stddev / sep1.averagePerLine);
                        }
                    });
                    
                    Separator separator = separators.get(0);
                    if (separator.stddev / separator.averagePerLine < 0.1) {
                        return separator.separator;
                    }
                   
                }
            } finally {
                lineNumberReader.close();
                reader.close();
                is.close();
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
        return 0;
    }


}
