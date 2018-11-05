package com.xx.stdb.index;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import com.xx.stdb.index.IndexEnums.IndexLevel;
import com.xx.stdb.index.IndexEnums.IndexType;
import com.xx.stdb.index.util.STIConstants;

import ch.hsr.geohash.GeoHash;

/**
 * @author dux(duxionggis@126.com)
 */
public class IndexZOrder implements ISTIndex {
	private static final IndexType TYPE = IndexType.ZORDER;

	private IndexLevel indexLevel = IndexLevel.MID;
	private IndexPrecision indexPrec = IndexPrecision.getPrecision(TYPE, indexLevel);

	public IndexZOrder(IndexLevel level) {
		this.setLevel(level);
	}

	public static String getVersion() {
		return "v1";
	}

	@Override
	public IndexPrecision getPrecision() {
		return this.indexPrec;
	}

	@Override
	public String encode(Coordinate coord, Date date) {
		if (coord == null) {
			throw new IllegalArgumentException("parameter coord is null");
		}

		GeoHash gh = GeoHash.withCharacterPrecision(coord.y, coord.x, indexPrec.getPrecision());
		return STIConstants.getCode(gh.toBase32(), indexPrec.getDateStr(date));
	}

	@Override
	public Set<String> encodes(Geometry geometry, Date date) {
		if (geometry == null || geometry.isEmpty()) {
			throw new IllegalArgumentException("parameter geometry is null");
		}

		Set<String> codes = new HashSet<>();
		if (geometry instanceof Point) {
			codes.add(encode(geometry.getCoordinate(), date));
			return codes;
		}

		Envelope bbox = geometry.getEnvelopeInternal();
		String dateStr = indexPrec.getDateStr(date);
		GeoHash gh;
		String code;
		String token;
		double maxx = bbox.getMaxX() + indexPrec.getInterval();
		double maxy = bbox.getMaxY() + indexPrec.getInterval();
		for (double lon = bbox.getMinX(); lon < maxx; lon = lon + indexPrec.getInterval()) {
			for (double lat = bbox.getMinY(); lat < maxy; lat = lat + indexPrec.getInterval()) {
				gh = GeoHash.withCharacterPrecision(lat, lon, indexPrec.getPrecision());
				token = gh.toBase32();
				code = STIConstants.getCode(token, dateStr);
				if (!codes.contains(code) && geometry.intersects(this.decode(token))) {
					codes.add(code);
				}
			}
		}
		return codes;
	}

	@Override
	public Polygon decode(String code) {
		if (code == null || code.isEmpty()) {
			throw new IllegalArgumentException("parameter code is invalid");
		}

		GeoHash gh = GeoHash.fromGeohashString(code);
		Coordinate[] coords = new Coordinate[5];
		double minx = format(gh.getBoundingBox().getMinLon());
		double miny = format(gh.getBoundingBox().getMinLat());
		double maxx = format(gh.getBoundingBox().getMaxLon());
		double maxy = format(gh.getBoundingBox().getMaxLat());
		coords[0] = new Coordinate(minx, miny);
		coords[1] = new Coordinate(minx, maxy);
		coords[2] = new Coordinate(maxx, maxy);
		coords[3] = new Coordinate(maxx, miny);
		coords[4] = coords[0];

		return geoFactory.createPolygon(coords);
	}

	public String toString() {
		return TYPE.name() + "_" + indexLevel.name();
	}

	private void setLevel(IndexLevel level) {
		if (level != null) {
			this.indexLevel = level;
			this.indexPrec = IndexPrecision.getPrecision(TYPE, this.indexLevel);
		}
	}

}
