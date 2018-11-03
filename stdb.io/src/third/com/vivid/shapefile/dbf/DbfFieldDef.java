package com.vivid.shapefile.dbf;

import java.io.IOException;

import com.vivid.shapefile.EndianDataInputStream;

public class DbfFieldDef implements DbfConsts {
	public StringBuilder fieldname = new StringBuilder(DBF_NAMELEN);
	public char fieldtype;
	public int fieldstart;
	public int fieldlen;
	public int fieldnumdec;

	public DbfFieldDef() { /* do nothing */
	}

	public DbfFieldDef(String fieldname, char fieldtype, int fieldlen,
			int fieldnumdec) {
		this.fieldname = new StringBuilder(fieldname);
		this.fieldname.setLength(DBF_NAMELEN);
		this.fieldtype = fieldtype;
		this.fieldlen = fieldlen;
		this.fieldnumdec = fieldnumdec;
	}

	public String toString() {
		return new String("" + fieldname + " " + fieldtype + " " + fieldlen
				+ "." + fieldnumdec);
	}

	public void setup(int pos, EndianDataInputStream dFile) throws IOException {
		byte[] strbuf = new byte[DBF_NAMELEN];
		int j = -1;
		int term = -1;
		for (int i = 0; i < DBF_NAMELEN; i++) {
			byte b = dFile.readByteLE();
			if (b == 0) {
				if (term == -1)
					term = j;
				continue;
			}
			j++;
			strbuf[j] = b; // <- read string's byte data
		}
		if (term == -1)
			term = j;
		String name = new String(strbuf, 0, term + 1);

		fieldname.append(name.trim());
		fieldtype = (char) dFile.readUnsignedByteLE();
		fieldstart = pos;
		dFile.skipBytes(4);
		switch (fieldtype) {
		case 'C':
		case 'c':
		case 'D':
		case 'L':
		case 'M':
		case 'G':
			fieldlen = (int) dFile.readUnsignedByteLE();
			fieldnumdec = (int) dFile.readUnsignedByteLE();
			fieldnumdec = 0;
			break;
		case 'N':
		case 'n':
		case 'F':
		case 'f':
			fieldlen = (int) dFile.readUnsignedByteLE();
			fieldnumdec = (int) dFile.readUnsignedByteLE();
			break;
		default:
			System.out.println("wrong field type: " + fieldtype);
		}
		
		dFile.skipBytes(14);

	}
}
