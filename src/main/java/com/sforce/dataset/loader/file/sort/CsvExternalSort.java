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
import com.sforce.dataset.loader.file.schema.ExternalFileSchema;
import com.sforce.dataset.util.DatasetUtils;

public class CsvExternalSort extends ExternalSort {
	
    public static File sortFile(File inputCsv, final Charset cs, final boolean distinct,final int headersize, ExternalFileSchema schema) throws IOException
    {
    	if(inputCsv==null || !inputCsv.canRead())
    	{
    		throw new IOException("File not found {"+inputCsv+"}");
    	}
    	
		File outputFile = new File(inputCsv.getParent(), FilenameUtils.getBaseName(inputCsv.getName())+ "_sorted." + FilenameUtils.getExtension(inputCsv.getName()));		
    			
//		ExternalFileSchema schema = ExternalFileSchema.load(inputCsv, cs);
		if(schema==null || schema.objects == null || schema.objects.size()==0 || schema.objects.get(0).fields == null)
		{
			throw new IOException("File does not have valid metadata json {"+ExternalFileSchema.getSchemaFile(inputCsv, System.out)+"}");
		}
		
		CsvRowComparator cmp = null;
//		try
//		{
			cmp = new CsvRowComparator(schema.objects.get(0).fields);
			if(cmp.getSortColumnCount()==0)
				return inputCsv;
//		}catch(Throwable t)
//		{
//			t.printStackTrace();
//			return inputCsv;
//		}
	
		List<File> l = sortInBatch(inputCsv, cs, cmp, distinct, headersize);
//		System.out.println("CsvExternalSort created " + l.size() + " tmp files");
		mergeSortedFiles(l, outputFile, cmp, cs, distinct, inputCsv, headersize);
		return outputFile;
    }

    /**
     * @param fbr
     *                data source
     * @param datalength
     *                estimated data volume (in bytes)
     * @param cmp
     *                string comparator
     * @param maxtmpfiles
     *                maximal number of temporary files
     * @param maxMemory
     *                maximum amount of memory to use (in bytes)
     * @param cs
     *                character set to use (can use
     *                Charset.defaultCharset())
     * @param tmpdirectory
     *                location of the temporary files (set to null for
     *                default location)
     * @param distinct
     *                Pass <code>true</code> if duplicate lines should be
     *                discarded.
     * @param numHeader
     *                number of lines to preclude before sorting starts
     * @param usegzip
     *                use gzip compression for the temporary files
     * @return a list of temporary flat files
     * @throws IOException
     */
    public static List<File> sortInBatch(File inputCsv, final Charset cs, CsvRowComparator cmp, final boolean distinct,final int numHeader) throws IOException 
    {
            List<File> files = new ArrayList<File>();
            long blocksize = estimateBestSizeOfBlocks(inputCsv.length(), DEFAULTMAXTEMPFILES, estimateAvailableMemory());// in bytes
			CsvListReader reader = new CsvListReader(new InputStreamReader(new BOMInputStream(new FileInputStream(inputCsv), false), DatasetUtils.utf8Decoder(null , cs )), CsvPreference.STANDARD_PREFERENCE);				
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
                                    files.add(sortAndSave(tmplist, cmp, cs, tmpdirectory, distinct));
                                    tmplist.clear();
                            }
                    } catch (EOFException oef) {
                            if (tmplist.size() > 0) {
                                    files.add(sortAndSave(tmplist, cmp, cs,tmpdirectory, distinct));
                                    tmplist.clear();
                            }
                    }
            } finally {
            	reader.close();
            }
            return files;
    }

    /**
     * Sort a list and save it to a temporary file
     * 
     * @return the file containing the sorted data
     * @param tmplist
     *                data to be sorted
     * @param cmp
     *                string comparator
     * @param cs
     *                charset to use for output (can use
     *                Charset.defaultCharset())
     * @param tmpdirectory
     *                location of the temporary files (set to null for
     *                default location)
     * @param distinct
     *                Pass <code>true</code> if duplicate lines should be
     *                discarded.
     * @param usegzip
     *                set to true if you are using gzip compression for the
     *                temporary files
     * @throws IOException
     */
	private static File sortAndSave(List<List<String>> tmplist,
			CsvRowComparator cmp, Charset cs, File tmpdirectory, boolean distinct)  throws IOException {
        Collections.sort(tmplist, cmp);
        File newtmpfile = File.createTempFile("sortInBatch",
                "flatfile", tmpdirectory);
        newtmpfile.deleteOnExit();
        CsvListWriter writer = new CsvListWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(newtmpfile), cs)),CsvPreference.STANDARD_PREFERENCE);
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
     * This merges a bunch of temporary flat files
     * 
     * @param files
     *                The {@link List} of sorted {@link File}s to be merged.
     * @param distinct
     *                Pass <code>true</code> if duplicate lines should be
     *                discarded. (elchetz@gmail.com)
     * @param outputfile
     *                The output {@link File} to merge the results to.
     * @param cmp
     *                The {@link Comparator} to use to compare
     *                {@link String}s.
     * @param cs
     *                The {@link Charset} to be used for the byte to
     *                character conversion.
     * @param append
     *                Pass <code>true</code> if result should append to
     *                {@link File} instead of overwrite. Default to be false
     *                for overloading methods.
     * @param usegzip
     *                assumes we used gzip compression for temporary files
     * @return The number of lines sorted. (P. Beaudoin)
     * @throws IOException
     * @since v0.1.4
     */
    public static int mergeSortedFiles(List<File> files, File outputfile,
            final Comparator<List<String>> cmp, Charset cs, boolean distinct, File inputfile,final int headersize) throws IOException {
            ArrayList<CsvFileBuffer> bfbs = new ArrayList<CsvFileBuffer>();
            for (File f : files) {
            	CsvFileBuffer bfb = new CsvFileBuffer(f, cs);
            	bfbs.add(bfb);
            }
            CsvListWriter writer = new CsvListWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputfile), cs)),CsvPreference.STANDARD_PREFERENCE);
            copyHeader(inputfile, writer, cs, headersize);
            int rowcounter = mergeSortedFiles(writer, cmp, distinct, bfbs);
            for (File f : files)
                    f.delete();
            return rowcounter;            
    }
    
    
    /**
     * This merges several BinaryFileBuffer to an output writer.
     * 
     * @param fbw
     *                A buffer where we write the data.
     * @param cmp
     *                A comparator object that tells us how to sort the
     *                lines.
     * @param distinct
     *                Pass <code>true</code> if duplicate lines should be
     *                discarded. (elchetz@gmail.com)
     * @param buffers
     *                Where the data should be read.
     * @return The number of lines sorted. (P. Beaudoin)
     * @throws IOException
     * 
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
     * @param fbr
     *                data source
     * @param datalength
     *                estimated data volume (in bytes)
     * @param cmp
     *                string comparator
     * @param maxtmpfiles
     *                maximal number of temporary files
     * @param maxMemory
     *                maximum amount of memory to use (in bytes)
     * @param cs
     *                character set to use (can use
     *                Charset.defaultCharset())
     * @param tmpdirectory
     *                location of the temporary files (set to null for
     *                default location)
     * @param distinct
     *                Pass <code>true</code> if duplicate lines should be
     *                discarded.
     * @param numHeader
     *                number of lines to preclude before sorting starts
     * @param usegzip
     *                use gzip compression for the temporary files
     * @return a list of temporary flat files
     * @throws IOException
     */
    public static void copyHeader(File inputCsv, CsvListWriter writer, Charset cs, final int numHeader) throws IOException 
    {
		CsvListReader reader = new CsvListReader(new InputStreamReader(new BOMInputStream(new FileInputStream(inputCsv), false), DatasetUtils.utf8Decoder(null , cs )), CsvPreference.STANDARD_PREFERENCE);				
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
        public CsvFileBuffer(File inputCsv, Charset cs) throws IOException {
			this.reader = new CsvListReader(new InputStreamReader(new BOMInputStream(new FileInputStream(inputCsv), false), DatasetUtils.utf8Decoder(null , cs )), CsvPreference.STANDARD_PREFERENCE);				
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
