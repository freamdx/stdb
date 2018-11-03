package com.vivid.shapefile.shp;

import java.io.IOException;

import com.vivid.shapefile.EndianDataInputStream;
import com.vivid.shapefile.EndianDataOutputStream;
import org.locationtech.jts.geom.*;

public class ShapefileHeader {
	private final static boolean DEBUG = false;
	public static final int MAGIC = 9994;
	public static final int VERSION = 1000;
	private int fileCode = -1;
	public int fileLength = -1;
	private int indexLength = -1;
	private int version = -1;
	private int shapeType = -1;
	// private double[] bounds = new double[4];
	private Envelope bounds;
	private double zmin = 0.0;
	private double zmax = 0.0;

	public ShapefileHeader(EndianDataInputStream file) throws IOException {
		// file.setLittleEndianMode(false);
		fileCode = file.readIntBE();
		for (int i = 0; i < 5; i++) {
			int tmp = file.readIntBE();
		}
		fileLength = file.readIntBE();

		// file.setLittleEndianMode(true);
		version = file.readIntLE();
		shapeType = file.readIntLE();

		// read in and for now ignore the bounding box
		for (int i = 0; i < 4; i++) {
			file.readDoubleLE();
		}

		// skip remaining unused bytes
		// file.setLittleEndianMode(false);
		file.skipBytes(32);

		if (fileCode != MAGIC) {
			System.err.println("Wrong magic number, expected " + MAGIC
					+ ", got " + fileCode);
		}
		if (version != VERSION) {
			System.err.println("Wrong version, expected " + MAGIC + ", got "
					+ version);
		}
	}

	public ShapefileHeader(GeometryCollection geometries, int dims) throws ShapefileException {
		int numShapes = geometries.getNumGeometries();
		if (numShapes == 0)
			throw new ShapefileException("geometries is null");

		ShapeHandler handle = Shapefile.getShapeType(
				geometries.getGeometryN(0), dims).getShapeHandler(
				geometries.getFactory());
		shapeType = handle.getShapeType().id;
		boolean zvalues = false;
		if (shapeType == ShapeType.POINTZ.id || shapeType == ShapeType.ARCZ.id
				|| shapeType == ShapeType.POLYGONZ.id
				|| shapeType == ShapeType.MULTIPOINTZ.id) {
			zvalues = true;
			zmin = Double.MAX_VALUE;
			zmax = Double.MIN_VALUE;
		}
		version = VERSION;
		fileCode = MAGIC;
		bounds = geometries.getEnvelopeInternal();
		fileLength = 0;
		for (int i = 0; i < numShapes; i++) {
			Geometry g = geometries.getGeometryN(i);
			fileLength += handle.getLength(g);
			fileLength += 4;// for each header
			if (zvalues) {
				Coordinate[] cc = g.getCoordinates();
				for (int j = 0; j < cc.length; j++) {
					if (Double.isNaN(cc[j].z))
						continue;
					if (cc[j].z < zmin)
						zmin = cc[j].z;
					if (cc[j].z > zmax)
						zmax = cc[j].z;
				}
			}
		}
		fileLength += 50;// space used by this, the main header
		indexLength = 50 + (4 * numShapes);
	}

	public void setFileLength(int fileLength) {
		this.fileLength = fileLength;
	}

	public void write(EndianDataOutputStream file) throws IOException {
		int pos = 0;
		// file.setLittleEndianMode(false);
		file.writeIntBE(fileCode);
		pos += 4;
		for (int i = 0; i < 5; i++) {
			file.writeIntBE(0);// Skip unused part of header
			pos += 4;
		}
		file.writeIntBE(fileLength);
		pos += 4;
		// file.setLittleEndianMode(true);
		file.writeIntLE(version);
		pos += 4;
		file.writeIntLE(shapeType);
		pos += 4;
		// write the bounding box
		file.writeDoubleLE(bounds.getMinX());
		file.writeDoubleLE(bounds.getMinY());
		file.writeDoubleLE(bounds.getMaxX());
		file.writeDoubleLE(bounds.getMaxY());
		pos += 8 * 4;

		// added by mmichaud on 4 nov. 2004
		file.writeDoubleLE(zmin);
		file.writeDoubleLE(zmax);
		pos += 8 * 2;
		// skip remaining unused bytes
		// file.setLittleEndianMode(false);//well they may not be unused
		// forever...
		// for(int i=0;i<2;i++){
		file.writeDoubleLE(0.0);
		file.writeDoubleLE(0.0);// Skip unused part of header
		pos += 8;
		// }

		if (DEBUG)
			System.out.println("Sfh->Position " + pos);
	}

	public void writeToIndex(EndianDataOutputStream file) throws IOException {
		int pos = 0;
		// file.setLittleEndianMode(false);
		file.writeIntBE(fileCode);
		pos += 4;
		for (int i = 0; i < 5; i++) {
			file.writeIntBE(0);// Skip unused part of header
			pos += 4;
		}
		file.writeIntBE(indexLength);
		pos += 4;
		// file.setLittleEndianMode(true);
		file.writeIntLE(version);
		pos += 4;
		file.writeIntLE(shapeType);
		pos += 4;
		// write the bounding box
		pos += 8;
		file.writeDoubleLE(bounds.getMinX());
		pos += 8;
		file.writeDoubleLE(bounds.getMinY());
		pos += 8;
		file.writeDoubleLE(bounds.getMaxX());
		pos += 8;
		file.writeDoubleLE(bounds.getMaxY());
		/*
		 * for(int i = 0;i<4;i++){ pos+=8; file.writeDouble(bounds[i]); }
		 */

		// skip remaining unused bytes
		// file.setLittleEndianMode(false);//well they may not be unused
		// forever...
		for (int i = 0; i < 4; i++) {
			file.writeDoubleLE(0.0);// Skip unused part of header
			pos += 8;
		}

		if (DEBUG)
			System.out.println("Sfh->Index Position " + pos);
	}

	public int getShapeType() {
		return shapeType;
	}

	public int getVersion() {
		return version;
	}

	public Envelope getBounds() {
		return bounds;
	}

	public String toString() {
		String res = new String("Sf-->type " + fileCode + " size " + fileLength
				+ " version " + version + " Shape Type " + shapeType);
		return res;
	}
}
