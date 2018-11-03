package com.vivid.shapefile;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * A class that gives most of the functionality of DataInputStream, but is
 * endian aware. Uses a real java.io.DataInputStream to actually do the writing.
 * 
 */
public class EndianDataInputStream {
	private DataInputStream inputStream;
	private byte[] workSpace = new byte[8];

	/** Creates new EndianDataInputStream */
	public EndianDataInputStream(InputStream in) {
		inputStream = new DataInputStream(in);
	}

	/** close the stream **/
	public void close() throws IOException {
		inputStream.close();
	}

	/** read a byte in BigEndian - the same as LE because its only 1 byte */
	public byte readByteBE() throws IOException {
		return inputStream.readByte();
	}

	/** read a byte in LittleEndian - the same as BE because its only 1 byte */
	public byte readByteLE() throws IOException {
		return inputStream.readByte();
	}

	/** read a byte in LittleEndian - the same as BE because its only 1 byte */
	public void readByteLEnum(byte[] b) throws IOException {
		inputStream.readFully(b);
	}

	/**
	 * read a byte in BigEndian - the same as LE because its only 1 byte.
	 * returns int as per java.io.DataStream
	 */
	public int readUnsignedByteBE() throws IOException {
		return inputStream.readUnsignedByte();
	}

	/**
	 * read a byte in LittleEndian - the same as BE because its only 1 byte.
	 * returns int as per java.io.DataStream
	 */
	public int readUnsignedByteLE() throws IOException {
		return inputStream.readUnsignedByte();
	}

	/** read a 16bit short in BE */
	public short readShortBE() throws IOException {
		return inputStream.readShort();
	}

	/** read a 16bit short in LE */
	public short readShortLE() throws IOException {
		inputStream.readFully(workSpace, 0, 2);

		return (short) (((workSpace[1] & 0xff) << 8) | (workSpace[0] & 0xff));
	}

	/** read a 32bit int in BE */
	public int readIntBE() throws IOException {
		return inputStream.readInt();
	}

	/** read a 32bit int in LE */
	public int readIntLE() throws IOException {
		inputStream.readFully(workSpace, 0, 4);

		return ((workSpace[3] & 0xff) << 24) | ((workSpace[2] & 0xff) << 16)
				| ((workSpace[1] & 0xff) << 8) | (workSpace[0] & 0xff);
	}

	/** read a 64bit long in BE */
	public long readLongBE() throws IOException {
		return inputStream.readLong();
	}

	/** read a 64bit long in LE */
	public long readLongLE() throws IOException {
		inputStream.readFully(workSpace, 0, 8);

		return ((long) (workSpace[7] & 0xff) << 56)
				| ((long) (workSpace[6] & 0xff) << 48)
				| ((long) (workSpace[5] & 0xff) << 40)
				| ((long) (workSpace[4] & 0xff) << 32)
				| ((long) (workSpace[3] & 0xff) << 24)
				| ((long) (workSpace[2] & 0xff) << 16)
				| ((long) (workSpace[1] & 0xff) << 8)
				| ((long) (workSpace[0] & 0xff));
	}

	/** read a 64bit double in BE */
	public double readDoubleBE() throws IOException {
		return inputStream.readDouble();
	}

	/** read a 64bit double in LE */
	public double readDoubleLE() throws IOException {
		long l;

		inputStream.readFully(workSpace, 0, 8);
		l = ((long) (workSpace[7] & 0xff) << 56)
				| ((long) (workSpace[6] & 0xff) << 48)
				| ((long) (workSpace[5] & 0xff) << 40)
				| ((long) (workSpace[4] & 0xff) << 32)
				| ((long) (workSpace[3] & 0xff) << 24)
				| ((long) (workSpace[2] & 0xff) << 16)
				| ((long) (workSpace[1] & 0xff) << 8)
				| ((long) (workSpace[0] & 0xff));

		return Double.longBitsToDouble(l);
	}

	/**
	 * skip ahead in the stream
	 * 
	 * @param num
	 *            number of bytes to read ahead
	 */
	public int skipBytes(int num) throws IOException {
		return inputStream.skipBytes(num);
	}
}
