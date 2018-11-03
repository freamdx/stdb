package com.xx.stdb.io.test;

import com.xx.stdb.base.feature.FeatureCollection;
import com.xx.stdb.io.shp.ShapefileReader;
import com.xx.stdb.io.shp.ShapefileWriter;

import java.util.Properties;

public class ShpfileTest {
	public static void main(String[] args) throws Exception {
		// read shp
		String inshp = "data/R.shp";
		Properties dp1 = new Properties();
		dp1.setProperty("charset", "gbk");
		dp1.setProperty("File", inshp);
		FeatureCollection fc = new ShapefileReader().read(dp1);
		System.out.println("read feature count: " + fc.size());

		// write shp
		String outshp = "d:/rd.shp";
		Properties dp2 = new Properties();
		dp2.setProperty("charset", "gbk");
		dp2.setProperty("File", outshp);
		new ShapefileWriter().write(fc, dp2);
		System.out.println("wrote features path: " + outshp);
	}

}
