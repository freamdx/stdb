package com.vivid.shapefile.shp;

import java.io.IOException;
import com.vivid.shapefile.EndianDataInputStream;
import com.vivid.shapefile.EndianDataOutputStream;
import org.locationtech.jts.geom.*;

public class PointHandler implements ShapeHandler {
	ShapeType myShapeType;
	GeometryFactory geometryFactory;

	public PointHandler(ShapeType type, GeometryFactory gf)
			throws ShapefileException {
		if ((type != ShapeType.POINT) && (type != ShapeType.POINTM)
				&& (type != ShapeType.POINTZ)) { // 2d, 2d+m, 3d+m
			throw new ShapefileException(
					"PointHandler constructor: expected a type of 1, 11 or 21");
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
			return geometryFactory.createPoint(new Coordinate(Double.NaN,
					Double.NaN, Double.NaN));
		}

		if (shapeType != myShapeType.id)
			throw new ShapefileException(
					"pointhandler.read() - handler's shapetype doesnt match file's");

		double x = file.readDoubleLE();
		double y = file.readDoubleLE();
		double m, z = Double.NaN;
		actualReadWords += 8;
		/*
		 * if ( shapeType ==11 ) { z= file.readDoubleLE(); actualReadWords += 4;
		 * } if ( shapeType >=11 ) { m = file.readDoubleLE(); actualReadWords +=
		 * 4; }
		 */
		// added on march, 24 by michael michaud
		// to read shapefile containing PointZ without m value
		if (myShapeType == ShapeType.POINTM) {
			m = file.readDoubleLE();
			actualReadWords += 4;
		} else if (myShapeType == ShapeType.POINTZ) {
			z = file.readDoubleLE();
			actualReadWords += 4;
			if (contentLength > actualReadWords) {
				m = file.readDoubleLE();
				actualReadWords += 8;
			}
		}

		// verify that we have read everything we need
		while (actualReadWords < contentLength) {
			file.readShortBE();
			actualReadWords += 1;
		}

		return geometryFactory.createPoint(new Coordinate(x, y, z));
	}

	public void write(Geometry geometry, EndianDataOutputStream file)
			throws IOException {
		// file.setLittleEndianMode(true);
		file.writeIntLE(getShapeType().id);
		Coordinate c = geometry.getCoordinates()[0];
		file.writeDoubleLE(c.x);
		file.writeDoubleLE(c.y);

		if (myShapeType == ShapeType.POINTZ) {
			if (Double.isNaN(c.z)) // nan means not defined
				file.writeDoubleLE(0.0);
			else
				file.writeDoubleLE(c.z);
		}
		if ((myShapeType == ShapeType.POINTZ)
				|| (myShapeType == ShapeType.POINTM)) {
			file.writeDoubleLE(-10E40); // M
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
		if (myShapeType == ShapeType.POINT)
			return 10;
		if (myShapeType == ShapeType.POINTM)
			return 14;
		return 18;
	}
}
