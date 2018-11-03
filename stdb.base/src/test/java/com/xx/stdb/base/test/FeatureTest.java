package com.xx.stdb.base.test;

import java.util.Date;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;

import com.xx.stdb.base.feature.AttributeType;
import com.xx.stdb.base.feature.Feature;
import com.xx.stdb.base.feature.FeatureWrapper;
import com.xx.stdb.base.feature.Schema;
import com.xx.stdb.base.feature.SimpleFeature;

public class FeatureTest {

	public static void main(String[] args) {
		GeometryFactory gf = new GeometryFactory();

		Schema s = new Schema();
		s.addAttribute("ID", AttributeType.STRING);
		s.addAttribute("BOOL", AttributeType.BOOLEAN);
		s.addAttribute("INT", AttributeType.INTEGER);
		s.addAttribute("FLA", AttributeType.FLOAT);
		s.addAttribute("LON", AttributeType.LONG);
		s.addAttribute("DOU", AttributeType.DOUBLE);
		s.addAttribute("DTG", AttributeType.DATE);

		System.out.println("-------- schema --------");
		Schema s1 = Schema.fromJson(s.toString());
		for (int i = 0; i < s.getAttributeCount(); i++) {
			System.out.println(s.getAttributeName(i) + "--" + s1.getAttributeName(i));
			System.out.println(s.getAttributeType(s.getAttributeName(i)) + "--" + s1.getAttributeType(i));
		}
		System.out.println();

		System.out.println("-------- feature --------");
		Feature f = new Feature(s, "1");
		f.setAttribute("ID", "1");
		f.setAttribute("BOOL", true);
		f.setAttribute("INT", 11);
		f.setAttribute("FLA", 11.1F);
		f.setAttribute("LON", 11111111111L);
		f.setAttribute("DOU", 11.1111);
		f.setAttribute("DTG", new Date());
		Coordinate[] coords = new Coordinate[2];
		coords[0] = new Coordinate(116.39, 39.91);
		coords[1] = new Coordinate(116.395, 39.915);
		f.setGeometry(gf.createLineString(coords));

		System.out.println(f.getAttribute("ID") + " -- " + f.getAttribute(1));
		System.out.println(f.getAttribute("DTG") + " -- " + f.getGeometry());

		SimpleFeature sf = FeatureWrapper.toSimple(f);
		f = FeatureWrapper.fromSimple(s, sf);
		System.out.println(f.getAttribute("ID") + " -- " + f.getAttribute(1));
		System.out.println(f.getAttribute("DTG") + " -- " + f.getGeometry());
		System.out.println();

		SimpleFeature sf1 = SimpleFeature.fromJson(sf.toString());
		System.out.println(sf);
		System.out.println(sf1);
		System.out.println();

		f = FeatureWrapper.fromSimple(s, sf);
		System.out.println(f.getAttribute("ID") + " -- " + f.getAttribute(1));
		System.out.println(f.getAttribute("DTG") + " -- " + f.getGeometry());
		System.out.println();

		System.out.println("-------- feature bytes --------");
		f.setGeometryCode(f.getGeometry().toText());
		byte[] bytes = FeatureWrapper.toBytes(f, true);
		System.out.println("to bytes size:" + bytes.length);
		f = FeatureWrapper.fromBytes(s, bytes, true);
		System.out.println(f.getAttribute("ID") + " -- " + f.getAttribute(1));
		System.out.println(f.getAttribute("DTG") + " -- " + f.getGeometryCode());
		System.out.println();
	}

}
