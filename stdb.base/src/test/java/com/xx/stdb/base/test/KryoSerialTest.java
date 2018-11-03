package com.xx.stdb.base.test;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.List;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;

import com.xx.stdb.base.KryoSerialization;
import com.xx.stdb.base.feature.AttributeType;
import com.xx.stdb.base.feature.Feature;
import com.xx.stdb.base.feature.FeatureWrapper;
import com.xx.stdb.base.feature.Schema;
import com.xx.stdb.base.feature.SimpleFeature;

public class KryoSerialTest {
	public static void main(String[] args) {
		KryoSerialization kryo = new KryoSerialization();
		int size = 1000;

		GeometryFactory gf = new GeometryFactory();
		Schema s = new Schema();
		s.addAttribute("ID", AttributeType.INTEGER);
		s.addAttribute("DTG", AttributeType.DATE);

		List<byte[]> kryoList = new ArrayList<>();

		kryo.register(SimpleFeature.class);
		long start = System.currentTimeMillis();
		for (int i = 0; i < size; ++i) {
			Feature f = new Feature(s);
			f.setAttribute("ID", i);
			f.setAttribute("DTG", new Date());
			f.setGeometry(gf.createPoint(new Coordinate(116.39, 39.91)));
			SimpleFeature sf = FeatureWrapper.toSimple(f);

			byte[] data = kryo.serialize(sf);
			kryoList.add(Base64.getEncoder().encode(data));
		}
		long end = System.currentTimeMillis() - start;
		System.out.println("kryo serialize time:" + end);

		long Dstart = System.currentTimeMillis();
		for (byte[] data : kryoList) {
			SimpleFeature sf = kryo.deserialize(Base64.getDecoder().decode(data));
			// Feature f = FeatureWrapper.fromSimple(s, sf);
			// System.out.println(f.getAttribute(0) + "--" + f.getAttribute(1));
		}
		long Dend = System.currentTimeMillis() - Dstart;
		System.out.println("kryo deserialize time:" + Dend);

	}

}
