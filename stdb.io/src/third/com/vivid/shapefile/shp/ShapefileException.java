package com.vivid.shapefile.shp;

/**
 * Thrown when an error relating to the shapefile occures
 */
public class ShapefileException extends Exception {
	private static final long serialVersionUID = 1L;

	public ShapefileException() {
		super();
	}

	public ShapefileException(String s) {
		super(s);
	}
}
