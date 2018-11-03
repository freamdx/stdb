package com.vivid.shapefile.shp;

import com.vivid.shapefile.EndianDataInputStream;
import com.vivid.shapefile.EndianDataOutputStream;
import org.locationtech.jts.geom.Geometry;

public interface ShapeHandler {
	public ShapeType getShapeType();

	public Geometry read(int contentLength, EndianDataInputStream file)
			throws java.io.IOException, ShapefileException;

	public void write(Geometry geometry, EndianDataOutputStream file)
			throws java.io.IOException;

	public int getLength(Geometry geometry); // length in 16bit words
}
