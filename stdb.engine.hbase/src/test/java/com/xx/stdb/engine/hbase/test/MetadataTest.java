package com.xx.stdb.engine.hbase.test;

import com.xx.stdb.base.feature.AttributeType;
import com.xx.stdb.base.feature.Schema;
import com.xx.stdb.engine.EngineException;
import com.xx.stdb.engine.IDataStore;
import com.xx.stdb.engine.hbase.HbaseDataStore;
import com.xx.stdb.index.IndexEnums.IndexLevel;
import com.xx.stdb.index.IndexEnums.IndexType;

public class MetadataTest {

	public static void main(String[] args) throws EngineException {
		String zkHost = "localhost";
		String zkPort = "2181";
		String collectionName = "trace_test";

		Schema s = new Schema();
		s.addAttribute("ID", AttributeType.STRING, true);
		s.addAttribute("BOO", AttributeType.BOOLEAN);
		s.addAttribute("INT", AttributeType.INTEGER);
		s.addAttribute("FLA", AttributeType.FLOAT);
		s.addAttribute("LON", AttributeType.LONG);
		s.addAttribute("DOU", AttributeType.DOUBLE);
		s.addAttribute("DTG", AttributeType.DATE, true);

		IDataStore dataStore = new HbaseDataStore(zkHost, zkPort);
		dataStore.addSTIndex(IndexType.HILBERT, IndexLevel.MID);
		dataStore.addSTIndex(IndexType.ZORDER, IndexLevel.MID);
		dataStore.setCollectionName(collectionName);

		try {
			System.out.println("----------create metadata------------");
			dataStore.createMetadata(s);
		} catch (EngineException ee) {
			ee.printStackTrace();
			System.out.println();
		}

		try {
			System.out.println("----------delete metadata------------");
			dataStore.setCollectionName("test"); // use true name to delete
			dataStore.deleteMetadata();
		} catch (EngineException ee) {
			ee.printStackTrace();
			System.out.println();
		}

		dataStore.destroy();
	}

}
