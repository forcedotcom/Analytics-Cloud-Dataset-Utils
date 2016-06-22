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
package com.sforce.dataset.loader.file.sort;

import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.input.BOMInputStream;
import org.supercsv.io.CsvListReader;
import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;

import com.google.code.externalsorting.ExternalSort;
import com.sforce.dataset.flow.monitor.Session;
import com.sforce.dataset.flow.monitor.ThreadContext;
import com.sforce.dataset.loader.file.schema.ext.ExternalFileSchema;
import com.sforce.dataset.util.DatasetUtils;
import com.sforce.dataset.util.FileUtilsExt;

/**
 * The Class CsvExternalSort.
 */
public class CsvExternalSort extends ExternalSort {
	
    /**
     * Sort file.
     *
     * @param inputCsv the input csv
     * @param cs the cs
     * @param distinct the distinct
     * @param headersize the headersize
     * @param schema the schema
     * @param pref the pref
     * @return the file
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public static File sortFile(File inputCsv, final Charset cs, final boolean distinct,final int headersize, ExternalFileSchema schema, CsvPreference pref) throws IOException
    {
    	if(inputCsv==null || !inputCsv.canRead())
    	{
    		throw new IOException("File not found {"+inputCsv+"}");
    	}
    	
		File outputFile = new File(inputCsv.getParent(), FilenameUtils.getBaseName(inputCsv.getName())+ "_sorted." + FilenameUtils.getExtension(inputCsv.getName()));		
    			
//		ExternalFileSchema schema = ExternalFileSchema.load(inputCsv, cs);
		if(schema==null || schema.getObjects() == null || schema.getObjects().size()==0 || schema.getObjects().get(0).getFields() == null)
		{
			throw new IOException("File does not have valid metadata json {"+ExternalFileSchema.getSchemaFile(inputCsv, System.out)+"}");
		}
		
		CsvRowComparator cmp = null;
//		try
//		{
			cmp = new CsvRowComparator(schema.getObjects().get(0).getFields());
			if(cmp.getSortColumnCount()==0)
				return inputCsv;
//		}catch(Throwable t)
//		{
//			t.printStackTrace();
//			return inputCsv;
//		}

		ThreadContext tx = ThreadContext.get();
		Session session = tx.getSession();
		session.setStatus("SORTING");
			
			
		List<File> l = sortInBatch(inputCsv, cs, cmp, distinct, headersize, pref);
//		System.out.println("CsvExternalSort created " + l.size() + " tmp files");
		mergeSortedFiles(l, outputFile, cmp, cs, distinct, inputCsv, headersize, pref);
		return outputFile;
    }

    /**
     * Sort in batch.
     *
     * @param inputCsv the input csv
     * @param cs the cs
     * @param cmp the cmp
     * @param distinct the distinct
     * @param numHeader the num header
     * @param pref the pref
     * @return the list
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public static List<File> sortInBatch(File inputCsv, final Charset cs, CsvRowComparator cmp, final boolean distinct,final int numHeader, CsvPreference pref) throws IOException 
    {
            List<File> files = new ArrayList<File>();
            long blocksize = estimateBestSizeOfBlocks(inputCsv.length(), DEFAULTMAXTEMPFILES, estimateAvailableMemory());// in bytes
			CsvListReader reader = new CsvListReader(new InputStreamReader(new BOMInputStream(new FileInputStream(inputCsv), false), DatasetUtils.utf8Decoder(null , cs )), pref);				
			File tmpdirectory = new File(inputCsv.getParent(),"archive");
			try
			{
				FileUtils.forceMkdir(tmpdirectory);
			}catch(Throwable t)
			{
				t.printStackTrace();
			}
			
            try {
                    List<List<String>> tmplist = new ArrayList<List<String>>();
					try {
                            int counter = 0;
    						List<String> row = new ArrayList<String>();
                            while (row != null) {
                                    long currentblocksize = 0;// in bytes                                    
                                    while ((currentblocksize < blocksize) && ((row = reader.read()) != null)) // as long as you have enough memory 
                                    {
                                            if (counter < numHeader) {
                                                    counter++;
                                                    continue;
                                            }
                                            tmplist.add(row);
                                            currentblocksize += row.size()*300;
                                    }
                                    files.add(sortAndSave(tmplist, cmp, cs, tmpdirectory, distinct, pref));
                                    tmplist.clear();
                            }
                    } catch (EOFException oef) {
                            if (tmplist.size() > 0) {
                                    files.add(sortAndSave(tmplist, cmp, cs,tmpdirectory, distinct, pref));
                                    tmplist.clear();
                            }
                    }
            } finally {
            	reader.close();
            }
            return files;
    }

    /**
     * Sort and save.
     *
     * @param tmplist the tmplist
     * @param cmp the cmp
     * @param cs the cs
     * @param tmpdirectory the tmpdirectory
     * @param distinct the distinct
     * @param pref the pref
     * @return the file
     * @throws IOException Signals that an I/O exception has occurred.
     */
	private static File sortAndSave(List<List<String>> tmplist,
			CsvRowComparator cmp, Charset cs, File tmpdirectory, boolean distinct, CsvPreference pref)  throws IOException {
        Collections.sort(tmplist, cmp);
        File newtmpfile = File.createTempFile("sortInBatch",
                "flatfile", tmpdirectory);
        newtmpfile.deleteOnExit();
        CsvListWriter writer = new CsvListWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(newtmpfile), cs)),pref);
        List<String> lastLine = null;
        try {
                for (List<String> rowData : tmplist) {
                        // Skip duplicate lines
                        if (!distinct || !rowData.equals(lastLine)) {
                        	writer.write(rowData);
                            lastLine = rowData;
                        }
                }
        } finally {
        	writer.close();
        }
        return newtmpfile;
	}
    

    /**
     * Merge sorted files.
     *
     * @param files the files
     * @param outputfile the outputfile
     * @param cmp the cmp
     * @param cs the cs
     * @param distinct the distinct
     * @param inputfile the inputfile
     * @param headersize the headersize
     * @param pref the pref
     * @return the int
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public static int mergeSortedFiles(List<File> files, File outputfile,
            final Comparator<List<String>> cmp, Charset cs, boolean distinct, File inputfile,final int headersize, CsvPreference pref) throws IOException {
            ArrayList<CsvFileBuffer> bfbs = new ArrayList<CsvFileBuffer>();
            for (File f : files) {
            	CsvFileBuffer bfb = new CsvFileBuffer(f, cs, pref);
            	bfbs.add(bfb);
            }
            CsvListWriter writer = new CsvListWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputfile), cs)),pref);
            copyHeader(inputfile, writer, cs, headersize, pref);
            int rowcounter = mergeSortedFiles(writer, cmp, distinct, bfbs);
            for (File f : files)
            	FileUtilsExt.deleteQuietly(f);
            return rowcounter;            
    }
    
    
    /**
     * Merge sorted files.
     *
     * @param writer the writer
     * @param cmp the cmp
     * @param distinct the distinct
     * @param buffers the buffers
     * @return the int
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public static int mergeSortedFiles(CsvListWriter writer,
            final Comparator<List<String>> cmp, boolean distinct,
            List<CsvFileBuffer> buffers) throws IOException {
            PriorityQueue<CsvFileBuffer> pq = new PriorityQueue<CsvFileBuffer>(
                    11, new Comparator<CsvFileBuffer>() {
                            @Override
                            public int compare(CsvFileBuffer i,
                            		CsvFileBuffer j) {
                                    return cmp.compare(i.peek(), j.peek());
                            }
                    });
            for (CsvFileBuffer bfb : buffers)
                    if (!bfb.empty())
                            pq.add(bfb);
            int rowcounter = 0;
            List<String> lastLine = null;
            try {
                    while (pq.size() > 0) {
                    	CsvFileBuffer bfb = pq.poll();
                            List<String> r = bfb.pop();
                            // Skip duplicate lines
                            if (!distinct || !r.equals(lastLine)) {
                            		writer.write(r);
                                    lastLine = r;
                            }
                            ++rowcounter;
                            if (bfb.empty()) {
                                    bfb.close();
                            } else {
                                    pq.add(bfb); // add it back
                            }
                    }
            } finally {
            		writer.close();
                    for (CsvFileBuffer bfb : pq)
                            bfb.close();
            }
            return rowcounter;

    }
    
    
    /**
     * Copy header.
     *
     * @param inputCsv the input csv
     * @param writer the writer
     * @param cs the cs
     * @param numHeader the num header
     * @param pref the pref
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public static void copyHeader(File inputCsv, CsvListWriter writer, Charset cs, final int numHeader, CsvPreference pref) throws IOException 
    {
		CsvListReader reader = new CsvListReader(new InputStreamReader(new BOMInputStream(new FileInputStream(inputCsv), false), DatasetUtils.utf8Decoder(null , cs )), pref);				
		try {
			int counter = 0;
			List<String> row = new ArrayList<String>();
			while ((counter < numHeader) && ((row = reader.read()) != null))
			{
				writer.write(row);
				counter++;
			}
		} catch (EOFException oef) {
		} finally {
			reader.close();
		}
    }
}


/**
 * This is essentially a thin wrapper on top of a CsvListReader... which keeps
 * the last line in memory.
 */
final class CsvFileBuffer {
        public CsvFileBuffer(File inputCsv, Charset cs, CsvPreference pref) throws IOException {
			this.reader = new CsvListReader(new InputStreamReader(new BOMInputStream(new FileInputStream(inputCsv), false), DatasetUtils.utf8Decoder(null , cs )), pref);				
            reload();
        }
        public void close() throws IOException {
                this.reader.close();
        }

        public boolean empty() {
                return this.cache == null;
        }

        public List<String> peek() {
                return this.cache;
        }

        public List<String> pop() throws IOException {
                List<String> answer = peek();// make a copy
                reload();
                return answer;
        }

        private void reload() throws IOException {
                this.cache = reader.read();
        }

        public CsvListReader reader;
        private List<String> cache;

}
