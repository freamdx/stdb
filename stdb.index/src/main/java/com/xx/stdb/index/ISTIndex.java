package com.xx.stdb.index;

import java.util.Date;
import java.util.Set;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;

import com.xx.stdb.index.IndexEnums.IndexLevel;

/**
 * @author dux(duxionggis@126.com)
 */
public interface ISTIndex {
	static final GeometryFactory geoFactory = new GeometryFactory();

	default double format(double ll) {
		return Math.round(ll * 1.0e7) / 1.0e7;
	}

	/**
	 * set IndexLevel
	 * 
	 * @param level
	 *            IndexLevel
	 */
	void setLevel(IndexLevel level);

	/**
	 * create spatio-temporal index code by coordinate
	 * 
	 * @param coord
	 *            Coordinate
	 * @param date
	 *            Date
	 * @return String
	 */
	String encode(Coordinate coord, Date date);

	/**
	 * create spatio-temporal index code list by geometry
	 * 
	 * @param geometry
	 *            Geometry
	 * @param date
	 *            Date
	 * @return Set<String>
	 */
	Set<String> encodes(Geometry geometry, Date date);

	/**
	 * get spatio-temporal index code's envelope
	 * 
	 * @param code
	 *            String, geohash or hilbert token
	 * @return Polygon
	 */
	Polygon decode(String code);

}
