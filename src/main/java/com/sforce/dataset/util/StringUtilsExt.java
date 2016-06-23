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
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;

public class StringUtilsExt {
	
//	private static final int EOF = -1;
	private static final char LF = '\n';
	private static final char CR = '\r';
	private static final char QUOTE = '"';
	private static final char COMMA = ',';

	public static final int DEFAULT_BUFFER_SIZE = 1024 * 4;	
	public static final Charset utf8Charset = Charset.forName("UTF-8");

	
	
//	public static String replaceSpecialCharacters(String inString) {
//		String outString = inString;
//		try 
//		{
//			if(inString != null && !inString.trim().isEmpty())
//			{
//				char[] outStr = new char[inString.length()];
//				int index = 0;
//				for(char ch:inString.toCharArray())
//				{
//					if(!Character.isLetterOrDigit((int)ch))
//					{
//						outStr[index] = '_';
//					}else
//					{
//						outStr[index] = ch;
//					}
//					index++;
//				}
//				outString = new String(outStr);
//			}
//		} catch (Throwable t) {
//			t.printStackTrace(Logger.out);
//		}
//		return outString;
//	}
	
	public static String getCSVFriendlyString(String content)
	{
		if(content!=null && !content.isEmpty())
		{
		content = replaceString(content, "" + COMMA, "");
		content = replaceString(content, "" + CR, "");
		content = replaceString(content, "" + LF, "");
		content = replaceString(content, "" + QUOTE, "");
		}
		return content;
	}

	public static String getHeartBeatFriendlyString(String content)
	{
		if(content!=null && !content.isEmpty())
		{
		content = replaceString(content, "" + CR, "");
		content = replaceString(content, "" + LF, "; ");
		content = replaceString(content, "" + '{', "[");
		content = replaceString(content, "" + '}', "]");
		content = replaceString(content, "" + QUOTE, "");
		}
		return content;
	}

	
	private static String replaceString(String original, String pattern, String replace) 
	{
		if(original != null && !original.isEmpty() && pattern != null && !pattern.isEmpty() && replace !=null)
		{
			final int len = pattern.length();
			int found = original.indexOf(pattern);

			if (found > -1) {
				StringBuffer sb = new StringBuffer();
				int start = 0;

				while (found != -1) {
					sb.append(original.substring(start, found));
					sb.append(replace);
					start = found + len;
					found = original.indexOf(pattern, start);
				}

				sb.append(original.substring(start));

				return sb.toString();
			} else {
				return original;
			}
		}else
			return original;
	}
	
	
//	public static String toString(Reader input) {
//		if(input == null)
//			return null;
//		try {
//			StringBuffer sbuf = new StringBuffer();
//			char[] cbuf = new char[DEFAULT_BUFFER_SIZE];
//			int count = -1;
//			try {
//				int n;
//				while ((n = input.read(cbuf)) != -1)
//				{
//					sbuf.append(cbuf, 0, n);
//					count = ((count == -1) ? n : (count + n));
//				}
//			} catch (IOException e) {
//				e.printStackTrace(Logger.out);
//			}
//			if (count == -1)
//				return null;
//			else
//				return sbuf.toString();
//		} finally {
//			if (input != null)
//				try {
//					input.close();
//				} catch (IOException e) {
//					e.printStackTrace(Logger.out);
//				}
//		}
//	}
	
//	public static byte[] toBytes(InputStream input) throws IOException {
//		if(input == null)
//			return null;
//		try 
//		{
//		ByteArrayOutputStream output = new ByteArrayOutputStream();
//		byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
//		int n = 0;
//		int count = -1;
//		while (EOF != (n = input.read(buffer))) {
//			output.write(buffer, 0, n);
//			count = ((count == -1) ? n : (count + n));
//		}
//		output.flush();
//		if(count == -1)
//			return null;
//		else
//			return output.toByteArray();
//		} finally {
//			if (input != null)
//				try {
//					input.close();
//				} catch (IOException e) {e.printStackTrace(Logger.out);}
//		}
//	} 
//	
//    public static byte[] serialize(Object obj) throws IOException {
//        ByteArrayOutputStream b = new ByteArrayOutputStream();
//        ObjectOutputStream o = new ObjectOutputStream(b);
//        o.writeObject(obj);
//        return b.toByteArray();
//    }
//
//
//    public static String padRight(String s, int n) {
//        return String.format("%1$-" + n + "s", s);  
//   }
//
//   public static String padLeft(String s, int n) {
//       return String.format("%1$" + n + "s", s);  
//   }
   
   
//	public static CharsetEncoder utf8Encoder(CodingErrorAction codingErrorAction) {
//	    try 
//	    {
//			if(codingErrorAction==null)
//				codingErrorAction = CodingErrorAction.REPLACE;
//		    final CharsetEncoder encoder = utf8Charset.newEncoder();
//		    encoder.reset();
//		    encoder.onUnmappableCharacter(codingErrorAction);
//		    encoder.onMalformedInput(codingErrorAction);
//	        return encoder;
//	    } catch (Throwable t) {
//	    	t.printStackTrace(Logger.out);
//	    	return null;
//	    }
//}

	public static CharsetDecoder utf8Decoder(CodingErrorAction codingErrorAction, String fileCharset) {
		return utf8Decoder(codingErrorAction, Charset.forName(fileCharset));
	}
	
	public static CharsetDecoder utf8Decoder(CodingErrorAction codingErrorAction) {
		return utf8Decoder(codingErrorAction, utf8Charset);
	}

	public static CharsetDecoder utf8Decoder(CodingErrorAction codingErrorAction, Charset fileCharset) {
    try 
    {
    	if(fileCharset == null)
    		fileCharset = utf8Charset;
		if(codingErrorAction==null)
			codingErrorAction = CodingErrorAction.REPLACE;
	    final CharsetDecoder encoder = fileCharset.newDecoder();
	    encoder.reset();
	    encoder.onUnmappableCharacter(codingErrorAction);
	    encoder.onMalformedInput(codingErrorAction);
        return encoder;
    } catch (Throwable t) {
    	t.printStackTrace(Logger.out);
    	return null;
    }
}
//
//public static byte[] toBytes(String value, CodingErrorAction codingErrorAction) {
//	if(value != null)
//	{
//	    try 
//	    {
//			if(codingErrorAction==null)
//				codingErrorAction = CodingErrorAction.REPLACE;
//		    final CharsetEncoder encoder = utf8Encoder(codingErrorAction);
//	        final ByteBuffer b = encoder.encode(CharBuffer.wrap(value));
//	        final byte[] bytes = new byte[b.remaining()];
//	        b.get(bytes);
//	        return bytes;
//	    } catch (Throwable t) {
//	    	t.printStackTrace(Logger.out);
//	    	return value.getBytes(utf8Charset);
//	    }
//	}
//    return null;
//}






}

