package com.xx.stdb.engine.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.xx.stdb.base.KryoSerialization;
import com.xx.stdb.base.feature.Feature;
import com.xx.stdb.base.feature.FeatureWrapper;
import com.xx.stdb.base.feature.Schema;
import com.xx.stdb.base.feature.SimpleFeature;
import com.xx.stdb.engine.EngineException;
import com.xx.stdb.engine.IDataSearcher;
import com.xx.stdb.engine.IDataStore;
import com.xx.stdb.engine.hbase.util.EHConstants;
import com.xx.stdb.engine.hbase.util.HbaseWrapper;
import com.xx.stdb.index.ISTIndex;
import com.xx.stdb.index.IndexEnums.IndexLevel;
import com.xx.stdb.index.IndexEnums.IndexType;
import com.xx.stdb.index.STIndexer;
import com.xx.stdb.index.util.GeoTransfer;
import com.xx.stdb.index.util.STIConstants;
import com.xx.stdb.index.util.StringUtil;

/**
 * @author dux(duxionggis@126.com)
 */
public class HbaseDataStore implements IDataStore {
	private String collectionName;
	private List<ISTIndex> indexers;
	private List<String> tabSTI;

	private HbaseWrapper hbase;
	private KryoSerialization kryo;

	public HbaseDataStore(String zkQuorum, String zkPort) throws EngineException {
		tabSTI = new ArrayList<>();
		indexers = new ArrayList<>();
		kryo = new KryoSerialization();
		kryo.register(SimpleFeature.class);
		try {
			hbase = new HbaseWrapper(zkQuorum, zkPort);
		} catch (IOException e) {
			throw new EngineException("connect hbase error:", e);
		}
	}

	@Override
	public void setCollectionName(String collectionName) {
		if (collectionName == null || collectionName.isEmpty()) {
			throw new IllegalArgumentException("parameter collection name is null");
		}
		this.collectionName = collectionName;
	}

	@Override
	public void addSTIndex(IndexType type, IndexLevel level) {
		if (type == null || level == null) {
			throw new IllegalArgumentException("parameter type or level is null");
		}

		String val = type.name() + "_" + level.name();
		String tab = "_" + val.toLowerCase() + "_" + STIConstants.getVersion(type);
		if (tabSTI.indexOf(tab) == -1) {
			tabSTI.add(tab);
			indexers.add(STIndexer.createIndexer(type, level));
		}
	}

	@Override
	public Schema getSchema() throws EngineException {
		try {
			Get get = new Get(EHConstants.DEF_KEY_SCHEMA_B);
			Result result = hbase.getValue(collectionName, get);
			byte[] bytes = result.getValue(EHConstants.DEF_COL_FAMILY_B, EHConstants.DEF_COL_CELL_B);

			return Schema.fromJson(Bytes.toString(bytes));
		} catch (IOException e) {
			throw new EngineException("get hbase schema error:", e);
		}
	}

	@Override
	public void createMetadata(Schema schema) throws EngineException {
		if (schema == null) {
			throw new IllegalArgumentException("parameter schema is null");
		}
		if (tabSTI.isEmpty()) {
			throw new EngineException("please add STIndex first");
		}

		// check table
		checkCollection("create metadate error:");

		// create table
		String collectionFID = collectionName + EHConstants.DEF_TAB_SUFFIX_FID;
		List<String> collectionSTI = new ArrayList<>();
		tabSTI.forEach(v -> collectionSTI.add(collectionName + v));
		try {
			hbase.createTable(collectionName, EHConstants.DEF_COL_FAMILY);
		} catch (IOException e) {
			throw new EngineException("create table " + collectionName + " error:", e);
		}
		// add meta data to table
		List<String> indexes = new ArrayList<>();
		indexers.forEach(v -> indexes.add(v.toString()));
		addMetadata(schema, collectionFID, collectionSTI, StringUtil.combine(indexes));

		// create feature id table
		try {
			hbase.createTable(collectionFID, EHConstants.DEF_COL_FAMILY);
		} catch (IOException e) {
			delTable(collectionName);
			delTable(collectionFID);
			throw new EngineException("create table " + collectionFID + " error:", e);
		}

		// create spatio-temporal index table
		collectionSTI.forEach(v -> {
			try {
				hbase.createTable(v, EHConstants.DEF_COL_FAMILY);
			} catch (IOException e) {
				delTable(collectionName);
				delTable(collectionFID);
				delTable(v);
			}
		});

	}

	@Override
	public void deleteMetadata() throws EngineException {
		// check table
		checkCollectionNo("delete metadate error:");

		// delete FID table
		try {
			Get get = new Get(EHConstants.DEF_KEY_TAB_FID_B);
			Result result = hbase.getValue(collectionName, get);
			byte[] fids = result.getValue(EHConstants.DEF_COL_FAMILY_B, EHConstants.DEF_COL_CELL_B);
			hbase.deleteTable(Bytes.toString(fids));
		} catch (IOException e) {
			// TODO log
		}
		// delete STI table
		try {
			Get get = new Get(EHConstants.DEF_KEY_TAB_STI_B);
			Result result = hbase.getValue(collectionName, get);
			byte[] bytes = result.getValue(EHConstants.DEF_COL_FAMILY_B, EHConstants.DEF_COL_CELL_B);
			String[] tabs = StringUtil.split(Bytes.toString(bytes), ";");
			for (String tab : tabs) {
				hbase.deleteTable(tab);
			}
		} catch (IOException e) {
			// TODO log
		}
		// delete table
		delTable(collectionName);

	}

	@Override
	public void writeFeature(Feature feature) throws EngineException {
		if (feature == null || feature.getGeometry() == null) {
			throw new IllegalArgumentException("parameter feature is null");
		}

		// check table
		checkCollectionNo("write feature error:");

		// rebuild properties
		Date date = feature.firstIndexedDateAttrib();
		date = date == null ? STIConstants.defaultDate() : date;
		if (indexers.isEmpty()) {
			buildIndexers();
		}
		String collectionFID = collectionName + EHConstants.DEF_TAB_SUFFIX_FID;
		List<String> collectionSTI = new ArrayList<>();
		tabSTI.forEach(v -> collectionSTI.add(collectionName + v));

		// feature to bytes
		byte[] data;
		if (EHConstants.KRYO_USED) {
			SimpleFeature sf = FeatureWrapper.toSimple(feature);
			sf.setWkt(GeoTransfer.toGeoHash(feature.getGeometry()));
			data = kryo.serialize(sf);
		} else {
			feature.setGeometryCode(GeoTransfer.toGeoHash(feature.getGeometry()));
			data = FeatureWrapper.toBytes(feature, EHConstants.FID_BUFFERED);
		}

		// write to FID table
		try {
			Put put = new Put(Bytes.toBytes(feature.getFid()));
			put.addColumn(EHConstants.DEF_COL_FAMILY_B, EHConstants.DEF_COL_CELL_B, data);
			hbase.setValue(collectionFID, put);
		} catch (IOException e) {
			throw new EngineException("write feature to FID table error:", e);
		}

		// write to STI table
		for (int i = 0; i < indexers.size(); i++) {
			String stiTab = collectionSTI.get(i);
			Set<String> stiSet = indexers.get(i).encodes(feature.getGeometry(), date);
			stiSet.forEach(v -> {
				try {
					Put put = new Put(Bytes.toBytes(EHConstants.getRowKey(v, feature.getFid())));
					put.addColumn(EHConstants.DEF_COL_FAMILY_B, EHConstants.DEF_COL_CELL_B, data);
					hbase.setValue(stiTab, put);
				} catch (Exception e) {
					// TODO log
				}
			});
		}
	}

	@Override
	public IDataSearcher createSearcher() throws EngineException {
		// check table
		checkCollectionNo("create searcher error:");

		// rebuild properties
		if (indexers.isEmpty()) {
			buildIndexers();
		}
		String collectionFID = collectionName + EHConstants.DEF_TAB_SUFFIX_FID;
		List<String> collectionSTI = new ArrayList<>();
		tabSTI.forEach(v -> collectionSTI.add(collectionName + v));

		return new HbaseDataSearcher(collectionFID, collectionSTI, indexers, hbase, kryo, getSchema());
	}

	@Override
	public void destroy() {
		try {
			hbase.close();
		} catch (IOException e) {
			// TODO log
		}
	}

	private void addMetadata(Schema schema, String collectionFID, List<String> collectionSTI, String indexes) {
		try {
			Put put = new Put(EHConstants.DEF_KEY_SCHEMA_B);
			put.addColumn(EHConstants.DEF_COL_FAMILY_B, EHConstants.DEF_COL_CELL_B, Bytes.toBytes(schema.toString()));
			hbase.setValue(collectionName, put);
		} catch (IOException e1) {
			// TODO log
		}
		try {
			Put put = new Put(EHConstants.DEF_KEY_STI_B);
			put.addColumn(EHConstants.DEF_COL_FAMILY_B, EHConstants.DEF_COL_CELL_B, Bytes.toBytes(indexes));
			hbase.setValue(collectionName, put);
		} catch (IOException e1) {
			// TODO log
		}
		try {
			Put put = new Put(EHConstants.DEF_KEY_TAB_FID_B);
			put.addColumn(EHConstants.DEF_COL_FAMILY_B, EHConstants.DEF_COL_CELL_B, Bytes.toBytes(collectionFID));
			hbase.setValue(collectionName, put);
		} catch (IOException e1) {
			// TODO log
		}
		try {
			Put put = new Put(EHConstants.DEF_KEY_TAB_STI_B);
			put.addColumn(EHConstants.DEF_COL_FAMILY_B, EHConstants.DEF_COL_CELL_B,
					Bytes.toBytes(StringUtil.combine(collectionSTI)));
			hbase.setValue(collectionName, put);
		} catch (IOException e1) {
			// TODO log
		}
	}

	private void buildIndexers() {
		try {
			Get get = new Get(EHConstants.DEF_KEY_STI_B);
			Result result = hbase.getValue(collectionName, get);
			byte[] bytes = result.getValue(EHConstants.DEF_COL_FAMILY_B, EHConstants.DEF_COL_CELL_B);
			String[] idxs = StringUtil.split(Bytes.toString(bytes), StringUtil.DEF_SPLIT);
			tabSTI.clear();
			indexers.clear();
			for (String idx : idxs) {
				String[] ii = StringUtil.split(idx, "_");
				if (ii.length == 2) {
					tabSTI.add("_" + idx.toLowerCase() + "_" + STIConstants.getVersion(IndexType.valueOf(ii[0])));
					indexers.add(STIndexer.createIndexer(IndexType.valueOf(ii[0]), IndexLevel.valueOf(ii[1])));
				}
			}
		} catch (IOException e) {
			// TODO log
		}
	}

	private void delTable(String table) {
		try {
			if (hbase.existTable(table)) {
				hbase.deleteTable(table);
			}
		} catch (IOException e) {
			// TODO log
		}
	}

	private void checkCollectionNo(String err) throws EngineException {
		try {
			if (!hbase.existTable(collectionName)) {
				throw new EngineException("hbase table " + collectionName + " is not exist");
			}
		} catch (IOException e) {
			throw new EngineException(err, e);
		}
	}

	private void checkCollection(String err) throws EngineException {
		try {
			if (hbase.existTable(collectionName)) {
				throw new EngineException("hbase table " + collectionName + " is exist");
			}
		} catch (IOException e) {
			throw new EngineException(err, e);
		}
	}

}
