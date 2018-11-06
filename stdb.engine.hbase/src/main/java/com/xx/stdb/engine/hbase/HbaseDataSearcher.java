package com.xx.stdb.engine.hbase;

import com.xx.stdb.base.KryoSerialization;
import com.xx.stdb.base.feature.*;
import com.xx.stdb.engine.EngineException;
import com.xx.stdb.engine.IDataSearcher;
import com.xx.stdb.engine.STFilter;
import com.xx.stdb.engine.hbase.util.EHConstants;
import com.xx.stdb.engine.hbase.util.HbaseWrapper;
import com.xx.stdb.engine.hbase.util.RowRegexUtil;
import com.xx.stdb.index.ISTIndex;
import com.xx.stdb.index.STIndexer;
import com.xx.stdb.index.util.GeoTransfer;
import com.xx.stdb.index.util.STIConstants;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

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
			return buildFeature(result, kryo, schema);
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
				features.add(buildFeature(result, kryo, schema));
			}
			if (features.isEmpty()) {
				throw new EngineException("get features null after resolving");
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
	public Set<Feature> spatioTemporalQuery(STFilter filter) throws EngineException {
		if (collectionSTI.isEmpty() || indexers.isEmpty()) {
			throw new EngineException("not found ISTIndex before spatio-temporal query");
		}
		if (filter == null) {
			throw new IllegalArgumentException("parameter filter is null");
		}
		if (filter.getGeometry() == null || filter.getGeometry().isEmpty()) {
			throw new IllegalArgumentException("filter.getGeometry is null or empty");
		}
		Date dateFrom = filter.getDateFrom();
		Date dateTo = filter.getDateTo();
		boolean nonNull = dateTo != null && dateFrom != null;
		if (nonNull) {
			if (dateTo.before(dateFrom)) {
				throw new IllegalArgumentException("filter's dateTo is before dateFrom");
			}
			if (((dateTo.getTime() - dateFrom.getTime()) / 86400000) > EHConstants.LIMIT_FILTER_DAYS) {
				throw new IllegalArgumentException("filter's date subtraction exceeded the default value");
			}
		}
		Set<Feature> fSet = new HashSet<>();

		int idx = 0; // the best index
		ISTIndex indexer = indexers.get(idx);
		Set<String> codes = indexer.encodes(filter.getGeometry(), null);
		SimpleDateFormat sdf = indexer.getPrecision().getDateFormat();
		int defLen = sdf.format(STIConstants.defaultDate()).length();
		Scan scan = buildScan(codes, filter.getFids(), dateFrom, dateTo, sdf);
		try {
			Table table = hbase.getTable(collectionSTI.get(idx));
			ResultScanner scanner = table.getScanner(scan);

			Result result;
			Feature feature;
			Date date;
			String code;
			while (fSet.size() < EHConstants.LIMIT_SCAN_SIZE && (result = scanner.next()) != null) {
				if (result.isEmpty()) {
					continue;
				}
				// filter by rowkey spatial code (rowkey date is not available)
				code = EHConstants.getSTCode(Bytes.toString(result.getRow()));
				if (!codes.contains(STIConstants.getToken(code, defLen))) {
					continue;
				}

				// filter by feature date and geometry
				feature = buildFeature(result, kryo, schema);
				date = feature.firstIndexedDateAttrib();
				boolean contains = true;
				if (nonNull && date != null) {
					contains = !(date.before(dateFrom) || date.after(dateTo));
				}
				if (contains && filter.getGeometry().intersects(feature.getGeometry())) {
					fSet.add(feature);
				}
			}
			scanner.close();
			table.close();
		} catch (IOException e) {
			throw new EngineException("spatio-temporal query error:", e);
		}
		return fSet;
	}

	private Scan buildScan(Set<String> codes, Set<String> fids, Date from, Date to, SimpleDateFormat sdf) {
		Scan scan = new Scan();
		scan.setMaxVersions();
		scan.setRaw(true);
		scan.setCaching(EHConstants.LIMIT_SCAN_SIZE / 2);
		scan.setBatch(EHConstants.LIMIT_SCAN_SIZE);

		String regexStr = RowRegexUtil.regex(codes, fids, from, to, sdf);
		RegexStringComparator comparator = new RegexStringComparator(regexStr);
		Filter filter = new RowFilter(CompareOp.EQUAL, comparator);
		scan.setFilter(filter);
		return scan;
	}

	static Feature buildFeature(Result result, KryoSerialization kryo, Schema schema) {
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
	}

}
