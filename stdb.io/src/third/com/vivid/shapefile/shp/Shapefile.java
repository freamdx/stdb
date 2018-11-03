package com.vivid.shapefile.shp;

import java.io.*;
import java.util.ArrayList;

import com.vivid.shapefile.EndianDataInputStream;
import com.vivid.shapefile.EndianDataOutputStream;
import org.locationtech.jts.geom.*;

public class Shapefile {
	private InputStream input;
	private OutputStream output;

	public void close() {
		try {
			input.close();
		} catch (IOException ex) {
		}
	}

	public void setInput(InputStream input) {
		if (input instanceof BufferedInputStream) {
			this.input = input;
		} else {
			this.input = new BufferedInputStream(input);
		}
	}

	public void setOutput(OutputStream output) {
		if (output instanceof BufferedOutputStream) {
			this.output = output;
		} else {
			this.output = new BufferedOutputStream(output);
		}
	}

	/**
	 * Initialises a shapefile from disk. Use Shapefile(String) if you don't
	 * want to use LEDataInputStream directly (recomended)
	 * 
	 * @param geometryFactory
	 *            the geometry factory to use to read the shapes
	 */
	public GeometryCollection read(GeometryFactory geometryFactory)
			throws IOException, ShapefileException {
		EndianDataInputStream file = new EndianDataInputStream(input);
		ShapefileHeader mainHeader = new ShapefileHeader(file);

		ArrayList<Geometry> list = new ArrayList<Geometry>();
		ShapeType type = ShapeType.forID(mainHeader.getShapeType());
		ShapeHandler handler = type.getShapeHandler(geometryFactory);
		if (handler == null)
			throw new ShapefileException("Unsuported shape type:" + type);

		int recordNumber = 0;
		int contentLength = 0;
		try {
			while (true) {
				// file.setLittleEndianMode(false);
				recordNumber = file.readIntBE();
				contentLength = file.readIntBE();
				try {
					Geometry body = handler.read(contentLength, file);
					list.add(body);
				} catch (Exception e) {
					list.add(geometryFactory
							.createGeometryCollection(new Geometry[0]));
				}
			}
		} catch (EOFException e) {
			// null
		}
		return geometryFactory.createGeometryCollection(list
				.toArray(new Geometry[] {}));
	}

	/**
	 * Saves a shapefile to and output stream.
	 * 
	 * @param geometries
	 *            geometry collection to write
	 * @param ShapeFileDimentions
	 *            shapefile dimension
	 * @throws IOException
	 * @throws ShapefileException
	 * @throws Exception
	 */
	// ShapeFileDimentions => 2=x,y ; 3=x,y,m ; 4=x,y,z,m
	public void write(GeometryCollection geometries, int ShapeFileDimentions)
			throws IOException, ShapefileException {
		EndianDataOutputStream file = new EndianDataOutputStream(output);
		ShapefileHeader mainHeader = new ShapefileHeader(geometries,
				ShapeFileDimentions);
		mainHeader.write(file);

		int pos = 50; // header length in WORDS
		int numShapes = geometries.getNumGeometries();
		if (numShapes == 0)
			throw new ShapefileException("geometries is null");

		ShapeHandler handler = Shapefile.getShapeType(
				geometries.getGeometryN(0), ShapeFileDimentions)
				.getShapeHandler(geometries.getFactory());

		for (int i = 0; i < numShapes; i++) {
			Geometry body = geometries.getGeometryN(i);
			// file.setLittleEndianMode(false);
			file.writeIntBE(i + 1);
			file.writeIntBE(handler.getLength(body));
			// file.setLittleEndianMode(true);
			pos += 4; // length of header in WORDS
			handler.write(body, file);
			pos += handler.getLength(body); // length of shape in WORDS
		}
		file.flush();
		file.close();
	}

	// ShapeFileDimentions => 2=x,y ; 3=x,y,m ; 4=x,y,z,m
	public static ShapeType getShapeType(Geometry geom, int ShapeFileDimentions)
			throws ShapefileException {

		if ((ShapeFileDimentions != 2) && (ShapeFileDimentions != 3)
				&& (ShapeFileDimentions != 4)) {
			throw new ShapefileException(
					"invalid ShapeFileDimentions for getShapeType - expected 2,3,or 4 but got "
							+ ShapeFileDimentions
							+ "  (2=x,y ; 3=x,y,m ; 4=x,y,z,m)");
			// ShapeFileDimentions = 2;
		}

		if (geom instanceof Point) {
			switch (ShapeFileDimentions) {
			case 2:
				return ShapeType.POINT;
			case 3:
				return ShapeType.POINTM;
			case 4:
				return ShapeType.POINTZ;
			}
		}
		if (geom instanceof MultiPoint) {
			switch (ShapeFileDimentions) {
			case 2:
				return ShapeType.MULTIPOINT;
			case 3:
				return ShapeType.MULTIPOINTM;
			case 4:
				return ShapeType.MULTIPOINTZ;
			}
		}
		if ((geom instanceof Polygon) || (geom instanceof MultiPolygon)) {
			switch (ShapeFileDimentions) {
			case 2:
				return ShapeType.POLYGON;
			case 3:
				return ShapeType.POLYGONM;
			case 4:
				return ShapeType.POLYGONZ;
			}
		}
		if ((geom instanceof LineString) || (geom instanceof MultiLineString)) {
			switch (ShapeFileDimentions) {
			case 2:
				return ShapeType.ARC;
			case 3:
				return ShapeType.ARCM;
			case 4:
				return ShapeType.ARCZ;
			}
		}
		return ShapeType.UNDEFINED;
	}

	public synchronized void readIndex(InputStream is) throws IOException {
		BufferedInputStream in = new BufferedInputStream(is);
		EndianDataInputStream file = new EndianDataInputStream(in);
		ShapefileHeader head = new ShapefileHeader(file);

		// file.setLittleEndianMode(false);
		file.close();
	}

	// ShapeFileDimentions => 2=x,y ; 3=x,y,m ; 4=x,y,z,m
	public synchronized void writeIndex(GeometryCollection geometries,
			EndianDataOutputStream file, int ShapeFileDimentions)
			throws ShapefileException, IOException {
		int numShapes = geometries.getNumGeometries();
		ShapefileHeader mainHeader = new ShapefileHeader(geometries,
				ShapeFileDimentions);

		if (numShapes == 0)
			throw new ShapefileException("geometries is null");

		ShapeHandler handler = Shapefile.getShapeType(
				geometries.getGeometryN(0), ShapeFileDimentions)
				.getShapeHandler(geometries.getFactory());

		// mainHeader.fileLength = 50 + 4*nrecords;
		mainHeader.writeToIndex(file);
		int pos = 50;
		int len = 0;

		// file.setLittleEndianMode(false);
		for (int i = 0; i < numShapes; i++) {
			Geometry geom = geometries.getGeometryN(i);
			len = handler.getLength(geom);

			file.writeIntBE(pos);
			file.writeIntBE(len);
			pos = pos + len + 4;
		}
		file.flush();
		file.close();
	}

}
