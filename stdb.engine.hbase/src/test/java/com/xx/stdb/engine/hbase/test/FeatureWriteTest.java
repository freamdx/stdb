package com.xx.stdb.engine.hbase.test;

import java.util.Date;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;

import com.xx.stdb.base.feature.Feature;
import com.xx.stdb.base.feature.Schema;
import com.xx.stdb.engine.EngineException;
import com.xx.stdb.engine.IDataStore;
import com.xx.stdb.engine.hbase.HbaseDataStore;

public class FeatureWriteTest {

	public static void main(String[] args) throws EngineException {
		GeometryFactory gf = new GeometryFactory();
		String zkHost = "localhost";
		String zkPort = "2181";
		String collectionName = "trace_test";

		IDataStore dataStore = new HbaseDataStore(zkHost, zkPort);
		dataStore.setCollectionName(collectionName);

		System.out.println("----------get schema------------");
		Schema s = dataStore.getSchema();
		System.out.println();

		System.out.println("----------write feature------------");
		long t1 = System.currentTimeMillis();
		for (int i = 0; i < 10000; i++) {
			Feature f = new Feature(s, String.valueOf(i));
			f.setAttribute("ID", String.valueOf(i));
			f.setAttribute("BOO", true);
			f.setAttribute("INT", 11);
			f.setAttribute("FLA", 11.1F);
			f.setAttribute("LON", 11111111111L);
			f.setAttribute("DOU", 11.1111);
			// f.setAttribute("DTG", null);
			f.setAttribute("DTG", new Date());
			Coordinate[] coords = new Coordinate[2];
			coords[0] = new Coordinate(116.39, 39.91);
			coords[1] = new Coordinate(116.3901 + i / 1.0e5, 39.9101 + i / 1.0e5);
			f.setGeometry(gf.createPoint(coords[1]));

			dataStore.writeFeature(f);
		}
		System.out.println("wrote time: " + (System.currentTimeMillis() - t1) / 1000 + " s");

		dataStore.destroy();
	}

}
