package com.vivid.shapefile.dbf;

import java.io.*;
import java.nio.charset.Charset;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.Date;
import com.vivid.shapefile.EndianDataInputStream;

public class DbfFile implements DbfConsts {
	int dbf_id;
	int last_update_d;
	int last_update_m;
	int last_update_y;
	int last_rec;
	int data_offset;
	int rec_size;
	EndianDataInputStream dFile;
	RandomAccessFile rFile;
	int filesize;
	int numfields;
	DbfFieldDef[] fielddef;
	Charset charset;

	protected DbfFile() {
	}

	/**
	 * Constructor, opens the file and reads the header infomation.
	 * 
	 * @param file
	 *            The file to be opened, includes path and .dbf
	 * @throws DbfFileException
	 * @throws IOException
	 */
	public DbfFile(String file, String charset_key) throws IOException,
			DbfFileException {
		EndianDataInputStream sfile = new EndianDataInputStream(
				new FileInputStream(file));
		rFile = new RandomAccessFile(new File(file), "r");
		init(sfile);
		charset = Charset.forName(charset_key);
	}

	/**
	 * Returns the date of the last update of the file as a string.
	 */
	public String getLastUpdate() {
		String date = last_update_d + "/" + last_update_m + "/" + last_update_y;
		return date;
	}

	/**
	 * Returns the number of records in the database file.
	 */
	public int getLastRec() {
		return last_rec;
	}

	/**
	 * Returns the size of the records in the database file.
	 */
	public int getRecSize() {
		return rec_size;
	}

	/**
	 * Returns the number of fields in the records in the database file.
	 */
	public int getNumFields() {
		return numfields;
	}

	public String getFieldName(int row) {
		return (fielddef[row].fieldname).toString();
	}

	public String getFieldType(int row) {
		char type = fielddef[row].fieldtype;
		String realtype = "";

		switch (type) {
		case 'C':
			realtype = "STRING";
			break;
		case 'N':
			if (fielddef[row].fieldnumdec == 0) {
				realtype = "INTEGER";
			} else {
				realtype = "DOUBLE";
			}
			break;
		case 'F':
			realtype = "DOUBLE";
			break;
		case 'D':
			realtype = "DATE";
			break;
		default:
			realtype = "STRING";
			break;
		}
		return realtype;
	}

	/**
	 * Returns the size of the database file.
	 */
	public int getFileSize() {
		return filesize;
	}

	/**
	 * initailizer, allows the use of multiple constructers in later versions.
	 */
	private void init(EndianDataInputStream sfile) throws IOException,
			DbfFileException {
		DbfFileHeader head = new DbfFileHeader(sfile);
		int widthsofar;
		dFile = sfile;
		fielddef = new DbfFieldDef[numfields];
		widthsofar = 1;

		for (int index = 0; index < numfields; index++) {
			fielddef[index] = new DbfFieldDef();
			fielddef[index].setup(widthsofar, dFile);
			widthsofar += fielddef[index].fieldlen;
		}
		sfile.skipBytes(1); // end of field defs marker
	}

	// public Object ParseRecordColumn(StringBuilder rec, int wantedCol)
	public Object ParseRecordColumn(byte[] rec, int wantedCol) throws Exception {
		int start = fielddef[wantedCol].fieldstart;
		int len = fielddef[wantedCol].fieldlen;
		int end = start + fielddef[wantedCol].fieldlen;

		switch (fielddef[wantedCol].fieldtype) {
		case 'C': // character
			while ((start < end) && (rec[end - 1] == ' '))
				end--;
			// return rec.substring(start, end);
			return new String(rec, start, end - start, charset);
		case 'F':
		case 'N': // numeric
			// String numb = rec.substring(start, end).trim();
			String numb = new String(rec, start, len).trim();
			if (fielddef[wantedCol].fieldnumdec == 0) { // its an int
				try {
					return new Integer(numb);
				} catch (NumberFormatException e) {
					return new Integer(0);
				}
			} else { // its a float
				try {
					return new Double(numb);
				} catch (NumberFormatException e) {
					return new Double(Double.NaN);
				}
			}
		case 'D':
			// return parseDate(rec.substring(start, end));
			return parseDate(new String(rec, start, len));
		default:
			// return rec.substring(start, end);
			return new String(rec, start, len, charset);
		}
	}

	/**
	 * fetches the <i>row</i>th row of the file
	 * 
	 * @param row
	 *            - the row to fetch
	 * @exception IOException
	 *                on read error.
	 */
	// public StringBuilder GetDbfRec(int row) IOException {
	// StringBuilder record = new StringBuilder(rec_size + numfields);
	public byte[] GetDbfRec(int row) throws IOException {
		rFile.seek(data_offset + (rec_size * row));

		// Multi byte character modification
		byte[] strbuf = new byte[rec_size];
		dFile.readByteLEnum(strbuf);
		// record.append(new String(strbuf)); // <- append byte array to String
		// return record;
		return strbuf;
	}

	public void close() throws IOException {
		dFile.close();
		rFile.close();
	}

	/**
	 * Internal Class to hold information from the header of the file
	 */
	class DbfFileHeader {
		/**
		 * Reads the header of a dbf file.
		 * 
		 * @param LEDataInputStream
		 *            file Stream attached to the input file
		 * @exception IOException
		 *                read error.
		 */
		public DbfFileHeader(EndianDataInputStream file) throws IOException {
			getDbfFileHeader(file);
		}

		private void getDbfFileHeader(EndianDataInputStream file)
				throws IOException {
			dbf_id = (int) file.readUnsignedByteLE();
			last_update_y = (int) file.readUnsignedByteLE() + DBF_CENTURY;
			last_update_m = (int) file.readUnsignedByteLE();
			last_update_d = (int) file.readUnsignedByteLE();
			last_rec = file.readIntLE();
			data_offset = file.readShortLE();
			rec_size = file.readShortLE();
			filesize = (rec_size * last_rec) + data_offset + 1;
			numfields = (data_offset - DBF_BUFFSIZE - 1) / DBF_BUFFSIZE;
			file.skipBytes(20);
		}
	}

	protected Date parseDate(String s) {
		if (s == null || s.trim().length() == 0)
			return null;
		try {
			if (s.equals("00000000"))
				return DATE_PARSER.parse("00010101");

			if (s.indexOf("/") > 0)
				DATE_PARSER_YYMMDD.parse(s);
			else
				return DATE_PARSER.parse(s);
		} catch (ParseException pe) {
			// ignore
		}
		return null;
	}

	public static final DateFormat DATE_PARSER = new SimpleDateFormat(
			"yyyyMMdd") {
		private static final long serialVersionUID = 1L;
		{
			setLenient(true);
		}
	};
	private static final DateFormat DATE_PARSER_YYMMDD = new SimpleDateFormat(
			"yy/mm/dd") {
		private static final long serialVersionUID = 1L;
		{
			setLenient(true);
		}
	};

}
