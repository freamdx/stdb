package com.xx.stdb.index.test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import com.xx.stdb.index.ISTIndex;
import com.xx.stdb.index.IndexEnums.IndexLevel;
import com.xx.stdb.index.IndexEnums.IndexType;
import com.xx.stdb.index.STIndexer;

public class STIndexTest {

	public static void main(String[] args) {
		GeometryFactory gf = new GeometryFactory();
		ISTIndex indexer;

		Date date = new Date();
		Coordinate coord = new Coordinate(116.4, 39.9);
		Coordinate coord1 = new Coordinate(116.401, 39.901);
		Coordinate coord2 = new Coordinate(116.401, 39.9);
		Coordinate[] coords = { coord, coord1, coord2, coord };

		List<String> stiStrs = new ArrayList<>();
		List<ISTIndex> stiIdxs = new ArrayList<>();

		LineString line = gf.createLineString(coords);
		Polygon poly = gf.createPolygon(coords);
		MultiPoint mpoint = gf.createMultiPointFromCoords(coords);
		Point point = gf.createPoint(coord);

		System.out.println(IndexType.HILBERT + " " + IndexLevel.HIGH);
		indexer = STIndexer.createIndexer(IndexType.HILBERT, IndexLevel.HIGH);
		print(date, coord, point, line, poly, mpoint, indexer);
		System.out.println();
		stiStrs.add("_" + indexer.toString().toLowerCase() + "_");
		stiIdxs.add(indexer);

		System.out.println(IndexType.HILBERT + " " + IndexLevel.MID);
		indexer = STIndexer.createIndexer(IndexType.HILBERT, IndexLevel.MID);
		print(date, coord, point, line, poly, mpoint, indexer);
		System.out.println();
		stiStrs.add("_" + indexer.toString().toLowerCase() + "_");
		stiIdxs.add(indexer);

		System.out.println(IndexType.HILBERT + " " + IndexLevel.LOW);
		indexer = STIndexer.createIndexer(IndexType.HILBERT, IndexLevel.LOW);
		print(null, coord, point, line, poly, mpoint, indexer);
		System.out.println();
		stiStrs.add("_" + indexer.toString().toLowerCase() + "_");
		stiIdxs.add(indexer);

		System.out.println(IndexType.ZORDER + " " + IndexLevel.HIGH);
		indexer = STIndexer.createIndexer(IndexType.ZORDER, IndexLevel.HIGH);
		print(date, coord, point, line, poly, mpoint, indexer);
		System.out.println();
		stiStrs.add("_" + indexer.toString().toLowerCase() + "_");
		stiIdxs.add(indexer);

		System.out.println(IndexType.ZORDER + " " + IndexLevel.MID);
		indexer = STIndexer.createIndexer(IndexType.ZORDER, IndexLevel.MID);
		print(date, coord, point, line, poly, mpoint, indexer);
		System.out.println();
		stiStrs.add("_" + indexer.toString().toLowerCase() + "_");
		stiIdxs.add(indexer);

		System.out.println(IndexType.ZORDER + " " + IndexLevel.LOW);
		indexer = STIndexer.createIndexer(IndexType.ZORDER, IndexLevel.LOW);
		print(null, coord, point, line, poly, mpoint, indexer);
		System.out.println();
		stiStrs.add("_" + indexer.toString().toLowerCase() + "_");
		stiIdxs.add(indexer);

		// compare
		Collections.sort(stiStrs, new STIndexer.STIComparator());
		Collections.sort(stiIdxs, new STIndexer.STIndexComparator());
		System.out.println(stiStrs);
		System.out.println(stiIdxs);
	}

	private static void print(Date date, Coordinate coord, Point point, LineString line, Polygon poly,
			MultiPoint mpoint, ISTIndex indexer) {
		String code = indexer.encode(coord, date);
		System.out.println(coord + " -- " + code);
		code = code.indexOf("2018") > 0 ? code.substring(0, code.indexOf("2018")) : code;
		System.out.println(code + " -- " + indexer.decode(code));
		Set<String> codes = indexer.encodes(line, date);
		System.out.println(line + " -- " + codes.size() + " -- " + codes);
		codes = indexer.encodes(poly, date);
		System.out.println(poly + " -- " + codes.size() + " -- " + codes);
		codes = indexer.encodes(mpoint, date);
		System.out.println(mpoint + " -- " + codes.size() + " -- " + codes);
		codes = indexer.encodes(point, date);
		System.out.println(point + " -- " + codes.size() + " -- " + codes);
	}

}
