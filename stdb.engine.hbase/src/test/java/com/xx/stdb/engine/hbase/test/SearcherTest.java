package com.xx.stdb.engine.hbase.test;

import java.util.List;
import java.util.Set;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import java.text.ParseException;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;

import com.xx.stdb.base.feature.Feature;
import com.xx.stdb.base.feature.FeatureIterator;
import com.xx.stdb.base.feature.FeatureWrapper;
import com.xx.stdb.engine.EngineException;
import com.xx.stdb.engine.IDataSearcher;
import com.xx.stdb.engine.IDataStore;
import com.xx.stdb.engine.STFilter;
import com.xx.stdb.engine.hbase.HbaseDataStore;
import com.xx.stdb.index.util.STIConstants;

public class SearcherTest {

	public static void main(String[] args) throws EngineException, ParseException {
		GeometryFactory gf = new GeometryFactory();
		String zkHost = "localhost";
		String zkPort = "2181";
		String collectionName = "trace_test";

		IDataStore dataStore = new HbaseDataStore(zkHost, zkPort);
		dataStore.setCollectionName(collectionName);

		System.out.println("----------get feature by fid------------");
		IDataSearcher searcher = dataStore.createSearcher();
		Feature f = searcher.get("2");
		System.out.println(FeatureWrapper.toSimple(f));

		System.out.println("----------get multi features------------");
		Set<String> fids = new HashSet<>();
		fids.add("1");
		fids.add("11");
		fids.add("2");
		fids.add("");
		List<Feature> features = searcher.multiGet(fids);
		System.out.println("input size " + fids.size() + ", output size " + features.size());

		System.out.println("----------get all features------------");
		int count = 0;
		FeatureIterator fi = searcher.readAll();
		while (fi.hasNext()) {
			if (count <= 10) {
				System.out.println(FeatureWrapper.toSimple(fi.next()).toString());
			}
			++count;
		}
		fi.close();
		System.out.println("readAll size:" + count);

		System.out.println("----------spatio-temporal query------------");
		Coordinate[] coords = new Coordinate[4];
		coords[0] = new Coordinate(116.39, 39.91);
		coords[1] = new Coordinate(116.391, 39.911);
		coords[2] = new Coordinate(116.392, 39.91);
		coords[3] = coords[0];
		Geometry geo = gf.createPolygon(coords);

		System.out.println("----------date and fids null------------");
		STFilter stfilter = new STFilter(geo, null, null);
		queryResolve(searcher, stfilter);

		System.out.println("----------date null------------");
		stfilter = new STFilter(geo, null, null);
		stfilter.addFid("40");
		stfilter.addFid("50");
		stfilter.addFid("60000");
		queryResolve(searcher, stfilter);

		System.out.println("----------fids null------------");
		Date date1 = STIConstants.SDF_HIGH.parse("20181103070101001");
		Date date2 = STIConstants.SDF_HIGH.parse("20181103175959999");
		stfilter = new STFilter(geo, date1, date2);
		queryResolve(searcher, stfilter);

		System.out.println("----------date and fids not null------------");
		stfilter = new STFilter(geo, date1, date2);
		stfilter.addFid("50");
		queryResolve(searcher, stfilter);

		dataStore.destroy();
	}

	private static void queryResolve(IDataSearcher searcher, STFilter stfilter) throws EngineException {
		Feature f;
		int count = 0;
		Iterator<Feature> it = searcher.spatioTemporalQuery(stfilter).iterator();
		while (it.hasNext()) {
			f = it.next();
			if (count <= 10) {
				System.out.println(FeatureWrapper.toSimple(f).toString());
			}
			++count;
		}
		System.out.println("query size:" + count);
		System.out.println();
	}

}
