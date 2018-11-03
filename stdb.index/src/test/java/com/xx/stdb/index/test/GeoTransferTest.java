package com.xx.stdb.index.test;

import java.util.Base64;
import java.util.Date;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import com.xx.stdb.base.KryoSerialization;
import com.xx.stdb.base.feature.AttributeType;
import com.xx.stdb.base.feature.Feature;
import com.xx.stdb.base.feature.FeatureWrapper;
import com.xx.stdb.base.feature.Schema;
import com.xx.stdb.base.feature.SimpleFeature;
import com.xx.stdb.index.util.GeoTransfer;

public class GeoTransferTest {
	private final static GeometryFactory gf = new GeometryFactory();

	public static void main(String[] args) {
		Coordinate c1 = new Coordinate(0, 0);
		Coordinate c2 = new Coordinate(4, 0);
		Coordinate c3 = new Coordinate(4, 4);
		Coordinate c4 = new Coordinate(0, 4);

		Coordinate b1 = new Coordinate(1, 1);
		Coordinate b2 = new Coordinate(3, 1);
		Coordinate b3 = new Coordinate(3, 3);
		Coordinate b4 = new Coordinate(1, 3);

		Coordinate[] cs = { c1, c2, c3, c4, c1 };
		Coordinate[] bs = { b1, b2, b3, b4, b1 };

		Point p = gf.createPoint(c1);
		MultiPoint mp = gf.createMultiPointFromCoords(cs);

		LineString l1 = gf.createLineString(cs);
		LineString l2 = gf.createLineString(bs);
		LineString[] ll = { l1, l2 };
		MultiLineString ml = gf.createMultiLineString(ll);

		LinearRing shell = gf.createLinearRing(cs);
		LinearRing[] holes = new LinearRing[1];
		holes[0] = gf.createLinearRing(bs);
		Polygon r = gf.createPolygon(shell, holes);

		Polygon[] rr = { r, gf.createPolygon(shell), gf.createPolygon(holes[0]) };
		MultiPolygon mr = gf.createMultiPolygon(rr);

		// /////////////////////////////////////////////////////////

		String hash = GeoTransfer.toGeoHash(p);
		System.out.println(p + "\t" + hash + "\t" + GeoTransfer.fromGeoHash(hash));

		hash = GeoTransfer.toGeoHash(mp);
		System.out.println(mp + "\t" + hash + "\t" + GeoTransfer.fromGeoHash(hash));

		hash = GeoTransfer.toGeoHash(l1);
		System.out.println(l1 + "\t" + hash + "\t" + GeoTransfer.fromGeoHash(hash));

		hash = GeoTransfer.toGeoHash(ml);
		System.out.println(ml + "\t" + hash + "\t" + GeoTransfer.fromGeoHash(hash));

		hash = GeoTransfer.toGeoHash(r);
		System.out.println(r + "\t" + hash + "\t" + GeoTransfer.fromGeoHash(hash));

		hash = GeoTransfer.toGeoHash(mr);
		System.out.println(mr + "\t" + hash + "\t" + GeoTransfer.fromGeoHash(hash));

		System.out.println();
		kryoSize();
	}

	private static void kryoSize() {
		KryoSerialization kryo = new KryoSerialization();
		kryo.register(SimpleFeature.class);

		Schema s = new Schema();
		s.addAttribute("ID", AttributeType.INTEGER);
		s.addAttribute("DTG", AttributeType.DATE);

		Feature f = new Feature(s, "1");
		f.setAttribute("ID", 1);
		f.setAttribute("DTG", new Date());
		f.setGeometry(gf.createPoint(new Coordinate(116.39, 39.91)));
		SimpleFeature sf = FeatureWrapper.toSimple(f);

		byte[] data1 = kryo.serialize(sf);
		byte[] data2 = Base64.getEncoder().encode(data1);
		System.out.println("kryo wkt length:" + data1.length + ", base64 length:" + data2.length);

		sf.setWkt(GeoTransfer.toGeoHash(f.getGeometry()));
		data1 = kryo.serialize(sf);
		data2 = Base64.getEncoder().encode(data1);
		System.out.println("kryo geohash length:" + data1.length + ", base64 length:" + data2.length);

		f.setGeometryCode(GeoTransfer.toGeoHash(f.getGeometry()));
		data1 = FeatureWrapper.toBytes(f, false);
		data2 = Base64.getEncoder().encode(data1);
		System.out.println("origin length:" + data1.length + ", base64 length:" + data2.length);
		f = FeatureWrapper.fromBytes(s, data1, false);
		f.setGeometry(GeoTransfer.fromGeoHash(f.getGeometryCode()));
	}

}
