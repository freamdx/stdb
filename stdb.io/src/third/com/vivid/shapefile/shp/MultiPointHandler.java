package com.vivid.shapefile.shp;

import java.io.IOException;

import com.vivid.shapefile.EndianDataInputStream;
import com.vivid.shapefile.EndianDataOutputStream;
import org.locationtech.jts.geom.*;

public class MultiPointHandler implements ShapeHandler {
	ShapeType myShapeType;
	GeometryFactory geometryFactory;

	public MultiPointHandler(ShapeType type, GeometryFactory gf)
			throws ShapefileException {
		if ((type != ShapeType.MULTIPOINT) && (type != ShapeType.MULTIPOINTM)
				&& (type != ShapeType.MULTIPOINTZ)) {
			throw new ShapefileException(
					"Multipointhandler constructor - expected type to be 8, 18, or 28");
		}

		this.myShapeType = type;
		this.geometryFactory = gf;
	}

	public Geometry read(int contentLength, EndianDataInputStream file)
			throws IOException, ShapefileException {
		// file.setLittleEndianMode(true);
		int actualReadWords = 0; // actual number of words read (word = 16bits)

		int shapeType = file.readIntLE();
		actualReadWords += 2;

		if (shapeType == 0) {
			return geometryFactory.createMultiPoint(new Coordinate[0]);
		}

		if (shapeType != myShapeType.id) {
			throw new ShapefileException(
					"Multipointhandler.read() - expected type code "
							+ myShapeType + " but got " + shapeType);
		}
		// read bbox
		file.readDoubleLE();
		file.readDoubleLE();
		file.readDoubleLE();
		file.readDoubleLE();

		actualReadWords += 4 * 4;

		int numpoints = file.readIntLE();
		actualReadWords += 2;

		Coordinate[] coords = new Coordinate[numpoints];
		for (int t = 0; t < numpoints; t++) {

			double x = file.readDoubleLE();
			double y = file.readDoubleLE();
			actualReadWords += 8;
			coords[t] = new Coordinate(x, y);
		}
		if (myShapeType == ShapeType.MULTIPOINTZ) {
			file.readDoubleLE(); // z min/max
			file.readDoubleLE();
			actualReadWords += 8;
			for (int t = 0; t < numpoints; t++) {
				double z = file.readDoubleLE();// z
				actualReadWords += 4;
				coords[t].z = z;
			}
		}

		if (myShapeType == ShapeType.MULTIPOINTM
				|| myShapeType == ShapeType.MULTIPOINTZ) {
			// int fullLength = numpoints * 8 + 20 +8 +4*numpoints + 8
			// +4*numpoints;
			int fullLength;
			if (myShapeType == ShapeType.MULTIPOINTZ) {
				// multipoint Z (with m)
				fullLength = 20 + (numpoints * 8) + 8 + 4 * numpoints + 8 + 4
						* numpoints;
			} else {
				// multipoint M (with M)
				fullLength = 20 + (numpoints * 8) + 8 + 4 * numpoints;
			}

			if (contentLength >= fullLength) // is the M portion actually there?
			{
				file.readDoubleLE(); // m min/max
				file.readDoubleLE();
				actualReadWords += 8;
				for (int t = 0; t < numpoints; t++) {
					file.readDoubleLE();// m
					actualReadWords += 4;
				}
			}
		}

		// verify that we have read everything we need
		while (actualReadWords < contentLength) {
			file.readShortBE();
			actualReadWords += 1;
		}

		return geometryFactory.createMultiPoint(coords);
	}

	double[] zMinMax(Geometry g) {
		double zmin, zmax;
		boolean validZFound = false;
		Coordinate[] cs = g.getCoordinates();
		double[] result = new double[2];

		zmin = Double.NaN;
		zmax = Double.NaN;
		double z;

		for (int t = 0; t < cs.length; t++) {
			z = cs[t].z;
			if (!(Double.isNaN(z))) {
				if (validZFound) {
					if (z < zmin)
						zmin = z;
					if (z > zmax)
						zmax = z;
				} else {
					validZFound = true;
					zmin = z;
					zmax = z;
				}
			}

		}

		result[0] = (zmin);
		result[1] = (zmax);
		return result;

	}

	public void write(Geometry geometry, EndianDataOutputStream file)
			throws IOException {
		MultiPoint mp = (MultiPoint) geometry;
		// file.setLittleEndianMode(true);
		file.writeIntLE(getShapeType().id);
		Envelope box = mp.getEnvelopeInternal();
		file.writeDoubleLE(box.getMinX());
		file.writeDoubleLE(box.getMinY());
		file.writeDoubleLE(box.getMaxX());
		file.writeDoubleLE(box.getMaxY());

		int numParts = mp.getNumGeometries();
		file.writeIntLE(numParts);

		for (int t = 0; t < mp.getNumGeometries(); t++) {
			Coordinate c = (mp.getGeometryN(t)).getCoordinate();
			file.writeDoubleLE(c.x);
			file.writeDoubleLE(c.y);
		}
		if (myShapeType == ShapeType.MULTIPOINTZ) {
			double[] zExtreame = zMinMax(mp);
			if (Double.isNaN(zExtreame[0])) {
				file.writeDoubleLE(0.0);
				file.writeDoubleLE(0.0);
			} else {
				file.writeDoubleLE(zExtreame[0]);
				file.writeDoubleLE(zExtreame[1]);
			}
			for (int t = 0; t < mp.getNumGeometries(); t++) {
				Coordinate c = (mp.getGeometryN(t)).getCoordinate();
				double z = c.z;
				if (Double.isNaN(z))
					file.writeDoubleLE(0.0);
				else
					file.writeDoubleLE(z);
			}
		}
		if (myShapeType == ShapeType.MULTIPOINTM
				|| myShapeType == ShapeType.MULTIPOINTZ) {
			file.writeDoubleLE(-10E40);
			file.writeDoubleLE(-10E40);
			for (int t = 0; t < mp.getNumGeometries(); t++) {
				file.writeDoubleLE(-10E40);
			}
		}
	}

	/**
	 * Returns the shapefile shape type value for a point
	 * 
	 * @return int Shapefile.POINT
	 */
	public ShapeType getShapeType() {
		return myShapeType;
	}

	/**
	 * Calcuates the record length of this object.
	 * 
	 * @return int The length of the record that this shapepoint will take up in
	 *         a shapefile
	 **/
	public int getLength(Geometry geometry) {
		MultiPoint mp = (MultiPoint) geometry;

		if (myShapeType == ShapeType.MULTIPOINT)
			return mp.getNumGeometries() * 8 + 20;
		else if (myShapeType == ShapeType.MULTIPOINTM)
			return mp.getNumGeometries() * 8 + 20 + 8 + 4
					* mp.getNumGeometries();

		return mp.getNumGeometries() * 8 + 20 + 8 + 4 * mp.getNumGeometries()
				+ 8 + 4 * mp.getNumGeometries();
	}
}
