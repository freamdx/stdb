package com.xx.stdb.io;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * @author dux(duxionggis@126.com)
 */
public class CompressedFile {
	public static final String FILE_KEY = "File";
	public static final String DEFAULT_CHARSET_KEY = "GBK";
	public static final String COMPRESSED_FILE_KEY = "CompressedFile";
	public static final String SHAPE_TYPE_KEY = "ShapeType";

	/**
	 * Utility file open function - handles compressed and un-compressed files.
	 * 
	 * @param fname
	 *            name of the file to search for.
	 * @param compressedFile
	 *            name of the compressed file.
	 */
	public static InputStream openFile(String fname, String compressedFile) throws Exception {
		if ((compressedFile == null) || (compressedFile.length() == 0)) {
			return new BufferedInputStream(new FileInputStream(fname));
		}

		String ext = compressedFile.substring(compressedFile.length() - 3);
		if (ext.compareToIgnoreCase(".gz") == 0) {
			return new GZIPInputStream(new FileInputStream(compressedFile));
		} else if (ext.compareToIgnoreCase("zip") == 0) {
			ZipInputStream fr_high = new ZipInputStream(new FileInputStream(compressedFile));
			ZipEntry entry = fr_high.getNextEntry();
			while (entry != null) {
				if (entry.getName().compareToIgnoreCase(fname) == 0) {
					return (fr_high);
				}
				entry = fr_high.getNextEntry();
			}

			throw new RuntimeException("couldnt find " + fname + " in compressed file " + compressedFile);
		}

		throw new RuntimeException(
				"Couldn't determine compressed file type for file " + compressedFile + " - Should end in .zip or .gz");
	}
}
