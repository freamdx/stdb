package com.xx.stdb.engine.hbase.test;

import java.util.List;
import java.util.Set;
import java.util.HashSet;

import com.xx.stdb.base.feature.Feature;
import com.xx.stdb.base.feature.FeatureIterator;
import com.xx.stdb.base.feature.FeatureWrapper;
import com.xx.stdb.engine.EngineException;
import com.xx.stdb.engine.IDataSearcher;
import com.xx.stdb.engine.IDataStore;
import com.xx.stdb.engine.hbase.HbaseDataStore;

public class SearcherTest {

	public static void main(String[] args) throws EngineException {
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

		dataStore.destroy();
	}

}
