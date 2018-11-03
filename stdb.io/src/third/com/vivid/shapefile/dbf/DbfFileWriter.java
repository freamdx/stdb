package com.vivid.shapefile.dbf;

import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.*;

import com.vivid.shapefile.EndianDataOutputStream;
import com.vivid.shapefile.FormatedString;

public class DbfFileWriter implements DbfConsts {
	int NoFields = 1;
	int NoRecs = 0;
	int recLength = 0;
	DbfFieldDef fields[];
	EndianDataOutputStream ls;
	private boolean header = false;
	Charset charset;

	public DbfFileWriter(String file, String charset_key) throws IOException {
		ls = new EndianDataOutputStream(new BufferedOutputStream(
				new FileOutputStream(file)));
		charset = Charset.forName(charset_key);
	}

	public void writeHeader(DbfFieldDef f[], int nrecs) throws IOException {
		NoFields = f.length;
		NoRecs = nrecs;
		fields = new DbfFieldDef[NoFields];
		for (int i = 0; i < NoFields; i++) {
			fields[i] = f[i];
		}
		ls.writeByteLE(3); // ID - dbase III with out memo

		// sort out the date
		Calendar calendar = new GregorianCalendar();
		Date trialTime = new Date();
		calendar.setTime(trialTime);
		ls.writeByteLE(calendar.get(Calendar.YEAR) - DBF_CENTURY);
		ls.writeByteLE(calendar.get(Calendar.MONTH) + 1); // month is 0-indexed
		ls.writeByteLE(calendar.get(Calendar.DAY_OF_MONTH));

		int dataOffset = 32 * NoFields + 32 + 1;
		for (int i = 0; i < NoFields; i++) {
			recLength += fields[i].fieldlen;
		}

		recLength++; // delete flag
		ls.writeIntLE(NoRecs);
		ls.writeShortLE(dataOffset); // length of header
		ls.writeShortLE(recLength);

		for (int i = 0; i < 20; i++)
			ls.writeByteLE(0); // 20 bytes of junk!

		// field descriptions
		for (int i = 0; i < NoFields; i++) {
			// patch from Hisaji Ono for Double byte characters
			ls.write(fields[i].fieldname.toString().getBytes(), 0, 11);
			ls.writeByteLE(fields[i].fieldtype);
			for (int j = 0; j < 4; j++)
				ls.writeByteLE(0); // junk
			ls.writeByteLE(fields[i].fieldlen);
			ls.writeByteLE(fields[i].fieldnumdec);
			for (int j = 0; j < 14; j++)
				ls.writeByteLE(0); // more junk
		}
		ls.writeByteLE(0xd);
		header = true;
	}

	public void writeRecord(Vector<Object> rec) throws DbfFileException,
			IOException {
		if (!header) {
			throw (new DbfFileException("Must write header before records"));
		}

		if (rec.size() != NoFields)
			throw new DbfFileException("wrong number of fields " + rec.size()
					+ " expected " + NoFields);
		ls.writeByteLE(' ');
		StringBuilder tmps;
		for (int i = 0; i < NoFields; i++) {
			int len = fields[i].fieldlen;
			Object o = rec.elementAt(i);
			switch (fields[i].fieldtype) {
			case 'C':
			case 'c':
			case 'D':
			case 'M':
			case 'G':
				// chars
				String ss = (String) o;
				while (ss.length() < fields[i].fieldlen) {
					// need to fill it with ' ' chars
					// this should converge quickly
					ss = ss
							+ "                                                                                                                  ";
				}
				tmps = new StringBuilder(ss);
				tmps.setLength(fields[i].fieldlen);
				// patch from Hisaji Ono for Double byte characters
				ls.write(tmps.toString().getBytes(charset),
						fields[i].fieldstart, fields[i].fieldlen);
				break;
			case 'N':
			case 'n':
				// int?
				String fs = "";
				if (fields[i].fieldnumdec == 0) {
					if (o instanceof Integer) {
						fs = FormatedString.format(((Integer) o).intValue(),
								fields[i].fieldlen);
					}
					// case LONG added by mmichaud on 18 sept. 2004
					else if (o instanceof Long) {
						fs = FormatedString.format(((Long) o).toString(), 0,
								fields[i].fieldlen);
					} else if (o instanceof BigDecimal) {
						fs = FormatedString.format(((BigDecimal) o).toString(),
								0, fields[i].fieldlen);
					} else
						;
					if (fs.length() > fields[i].fieldlen)
						fs = FormatedString.format(0, fields[i].fieldlen);
					ls.writeBytesLE(fs);
					break;
				} else {
					if (o instanceof Double) {
						fs = FormatedString.format(((Double) o).toString(),
								fields[i].fieldnumdec, fields[i].fieldlen);
					} else if (o instanceof BigDecimal) {
						fs = FormatedString.format(((BigDecimal) o).toString(),
								fields[i].fieldnumdec, fields[i].fieldlen);
					} else
						;
					if (fs.length() > fields[i].fieldlen)
						fs = FormatedString.format("0.0",
								fields[i].fieldnumdec, fields[i].fieldlen);
					ls.writeBytesLE(fs);
					break;
				}
			case 'F':
			case 'f':
				// double
				String s = ((Double) o).toString();
				String x = FormatedString.format(s, fields[i].fieldnumdec,
						fields[i].fieldlen);
				ls.writeBytesLE(x);
				break;
			// Case 'logical' added by mmichaud on 18 sept. 2004
			case 'L':
				// boolean
				if (o == null || o.equals("") || o.equals(" "))
					ls.writeBytesLE(" ");
				else {
					boolean b = ((Boolean) o).booleanValue();
					ls.writeBytesLE(b ? "T" : "F");
				}
				break;
			}// switch
		}// fields
	}

	public void close() throws IOException {
		ls.writeByteLE(0x1a); // eof mark
		ls.close();
	}

}
