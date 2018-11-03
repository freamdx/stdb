package com.xx.stdb.io;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Iterator;

/**
 * @author dux(duxionggis@126.com)
 */
public class FileIterator implements Iterator<String> {
	protected String file;
	protected String charset;

	protected BufferedReader reader;
	protected String item;
	protected int lineNumber;
	protected boolean closed = false;

	public FileIterator(String file, String charset) {
		if (file == null || file.trim().length() == 0)
			throw new IllegalArgumentException("file name is null");
		try {
			this.file = file;
			this.charset = charset;
		} catch (Exception e) {
			throw new RuntimeException("build reader error: ", e);
		}
	}

	public boolean hasNext() {
		if (closed)
			return false;
		if (item != null)
			return true;
		tryNext();
		return (!closed && item != null);
	}

	private void tryNext() {
		try {
			if (lineNumber == 0) {
				reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), charset));
			}
			if (reader != null) {
				item = reader.readLine();
				if (item == null) {
					close();
				} else {
					lineNumber++;
				}
			}
		} catch (Exception e) {
			throw new RuntimeException("read url error: ", e);
		}
	}

	public String next() {
		if (!hasNext())
			throw new IllegalStateException();
		String last = item;
		item = null;
		return last;
	}

	public void remove() {
		// do nothing
	}

	public int getLineNumber() {
		return lineNumber;
	}

	public String getLine() {
		return item;
	}

	public void pushback(String line) {
		closed = false;
		item = line;
	}

	public void close() {
		closed = true;
		if (reader != null) {
			try {
				reader.close();
			} catch (Exception ce) {
			} finally {
				reader = null;
			}
		}
	}

}