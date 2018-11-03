package com.xx.stdb.index;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import com.google.common.geometry.S2Cell;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2LatLng;
import com.xx.stdb.index.IndexEnums.IndexLevel;
import com.xx.stdb.index.IndexEnums.IndexType;
import com.xx.stdb.index.util.STIConstants;

/**
 * @author dux(duxionggis@126.com)
 */
public class IndexHilbert implements ISTIndex {
	private static final IndexType TYPE = IndexType.HILBERT;

	private IndexLevel indexLevel = IndexLevel.MID;
	private IndexPrecision indexPrec = IndexPrecision.getPrecision(TYPE, indexLevel);

	public IndexHilbert(IndexLevel level) {
		this.setLevel(level);
	}

	public static String getVersion() {
		return "v1";
	}

	@Override
	public void setLevel(IndexLevel level) {
		if (level != null) {
			this.indexLevel = level;
			this.indexPrec = IndexPrecision.getPrecision(TYPE, this.indexLevel);
		}
	}

	@Override
	public String encode(Coordinate coord, Date date) {
		if (coord == null) {
			throw new IllegalArgumentException("parameter coord is null");
		}

		S2CellId cid = S2CellId.fromLatLng(S2LatLng.fromDegrees(coord.y, coord.x)).parent(indexPrec.getPrecision());
		return STIConstants.getCode(cid.toToken(), indexPrec.getDateStr(date));
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
		S2CellId cid;
		String code;
		String token;
		double maxx = bbox.getMaxX() + indexPrec.getInterval();
		double maxy = bbox.getMaxY() + indexPrec.getInterval();
		for (double lon = bbox.getMinX(); lon < maxx; lon = lon + indexPrec.getInterval()) {
			for (double lat = bbox.getMinY(); lat < maxy; lat = lat + indexPrec.getInterval()) {
				cid = S2CellId.fromLatLng(S2LatLng.fromDegrees(lat, lon)).parent(indexPrec.getPrecision());
				token = cid.toToken();
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

		S2CellId cid = S2CellId.fromToken(code);
		S2Cell cell = new S2Cell(cid);
		Coordinate[] coords = new Coordinate[5];
		for (int k = 0; k < 4; k++) {
			S2LatLng sll = new S2LatLng(cell.getVertex(k));
			coords[k] = new Coordinate(format(sll.lngDegrees()), format(sll.latDegrees()));
		}
		coords[4] = coords[0];
		return geoFactory.createPolygon(coords);
	}

	public String toString() {
		return TYPE.name() + "_" + indexLevel.name();
	}

}
