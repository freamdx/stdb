package com.xx.stdb.engine.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import com.xx.stdb.base.KryoSerialization;
import com.xx.stdb.base.feature.Feature;
import com.xx.stdb.base.feature.FeatureIterator;
import com.xx.stdb.base.feature.FeatureWrapper;
import com.xx.stdb.base.feature.Schema;
import com.xx.stdb.base.feature.SimpleFeature;
import com.xx.stdb.engine.EngineException;
import com.xx.stdb.engine.IDataSearcher;
import com.xx.stdb.engine.STFilter;
import com.xx.stdb.engine.hbase.util.EHConstants;
import com.xx.stdb.engine.hbase.util.HbaseWrapper;
import com.xx.stdb.index.ISTIndex;
import com.xx.stdb.index.STIndexer;
import com.xx.stdb.index.util.GeoTransfer;

/**
 * @author dux(duxionggis@126.com)
 */
public class HbaseDataSearcher implements IDataSearcher {
	private String collectionFID;
	private List<String> collectionSTI;
	private List<ISTIndex> indexers;

	private HbaseWrapper hbase;
	private KryoSerialization kryo;
	private Schema schema;

	HbaseDataSearcher(String collectionFID, List<String> collectionSTI, List<ISTIndex> indexers, HbaseWrapper hbase,
			KryoSerialization kryo, Schema schema) {
		this.collectionFID = collectionFID;
		this.collectionSTI = collectionSTI;
		this.indexers = indexers;

		this.hbase = hbase;
		this.kryo = kryo;
		this.schema = schema;

		Collections.sort(this.collectionSTI, new STIndexer.STIComparator());
		Collections.sort(indexers, new STIndexer.STIndexComparator());
	}

	@Override
	public Feature get(String fid) throws EngineException {
		if (fid == null || fid.isEmpty()) {
			throw new IllegalArgumentException("parameter feature id is null");
		}

		try {
			Get get = new Get(Bytes.toBytes(fid));
			Result result = hbase.getValue(collectionFID, get);
			if (result == null || result.isEmpty()) {
				throw new EngineException("get feature null");
			}

			byte[] bytes = result.getValue(EHConstants.DEF_COL_FAMILY_B, EHConstants.DEF_COL_CELL_B);
			Feature f;
			if (EHConstants.KRYO_USED) {
				SimpleFeature sf = kryo.deserialize(bytes);
				f = FeatureWrapper.fromSimple(schema, sf);
				f.setGeometry(GeoTransfer.fromGeoHash(sf.getWkt()));
			} else {
				f = FeatureWrapper.fromBytes(schema, bytes, EHConstants.FID_BUFFERED);
				f.setGeometry(GeoTransfer.fromGeoHash(f.getGeometryCode()));
				f.setGeometryCode(null);
				if (!EHConstants.FID_BUFFERED) {
					f.setFid(EHConstants.getFid(Bytes.toString(result.getRow())));
				}
			}
			return f;
		} catch (IOException e) {
			throw new EngineException("get feature by fid error:", e);
		}
	}

	@Override
	public List<Feature> multiGet(Set<String> fids) throws EngineException {
		if (fids == null || fids.isEmpty()) {
			throw new IllegalArgumentException("parameter list feature id is null");
		}

		List<Get> gets = new ArrayList<>(fids.size());
		for (String fid : fids) {
			if (fid != null && !fid.isEmpty()) {
				gets.add(new Get(Bytes.toBytes(fid)));
			}
		}
		try {
			Result[] rs = hbase.multiGet(collectionFID, gets);
			if (rs == null || rs.length == 0) {
				throw new EngineException("get features null after multiGetting");
			}

			List<Feature> features = new ArrayList<>(rs.length);
			for (Result result : rs) {
				if (result == null || result.isEmpty()) {
					continue;
				}
				byte[] bytes = result.getValue(EHConstants.DEF_COL_FAMILY_B, EHConstants.DEF_COL_CELL_B);
				Feature f;
				if (EHConstants.KRYO_USED) {
					SimpleFeature sf = kryo.deserialize(bytes);
					f = FeatureWrapper.fromSimple(schema, sf);
					f.setGeometry(GeoTransfer.fromGeoHash(sf.getWkt()));
				} else {
					f = FeatureWrapper.fromBytes(schema, bytes, EHConstants.FID_BUFFERED);
					f.setGeometry(GeoTransfer.fromGeoHash(f.getGeometryCode()));
					f.setGeometryCode(null);
					if (!EHConstants.FID_BUFFERED) {
						f.setFid(EHConstants.getFid(Bytes.toString(result.getRow())));
					}
				}
				features.add(f);
			}
			if (features.isEmpty()) {
				throw new EngineException("get features null after Resolving");
			}
			return features;
		} catch (IOException e) {
			throw new EngineException("get features error:", e);
		}
	}

	@Override
	public FeatureIterator readAll() throws EngineException {
		Scan scan = new Scan();
		scan.setMaxVersions();
		scan.setRaw(true);
		try {
			Table table = hbase.getTable(collectionFID);
			ResultScanner scanner = table.getScanner(scan);
			return new HbaseFeatureIterator(kryo, schema, table, scanner);
		} catch (IOException e) {
			throw new EngineException("get all features error:", e);
		}
	}

	@Override
	public FeatureIterator spatioTemporalQuery(STFilter filter) throws EngineException {
		if (collectionSTI.isEmpty() || indexers.isEmpty()) {
			throw new EngineException("not found ISTIndex before spatio-temporal query");
		}

		int idx = 0; // the best index
		Scan scan = buildScan(filter, indexers.get(idx));
		try {
			Table table = hbase.getTable(collectionSTI.get(idx));
			ResultScanner scanner = table.getScanner(scan);
			return new HbaseFeatureIterator(kryo, schema, table, scanner);
		} catch (IOException e) {
			throw new EngineException("spatio-temporal query error:", e);
		}
	}

	private Scan buildScan(STFilter stiFilter, ISTIndex indexer) throws EngineException {
		if (stiFilter == null) {
			throw new IllegalArgumentException("parameter filter is null");
		}
		if (stiFilter.getGeometry() == null || stiFilter.getGeometry().isEmpty()) {
			throw new IllegalArgumentException("filter.getGeometry is null or empty");
		}

		Scan scan = new Scan();
		scan.setMaxVersions();
		scan.setRaw(true);
		// TODO
		return scan;
	}

}
