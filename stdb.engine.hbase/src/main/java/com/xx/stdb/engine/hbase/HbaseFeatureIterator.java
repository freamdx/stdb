package com.xx.stdb.engine.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.xx.stdb.base.KryoSerialization;
import com.xx.stdb.base.feature.Feature;
import com.xx.stdb.base.feature.FeatureIterator;
import com.xx.stdb.base.feature.FeatureWrapper;
import com.xx.stdb.base.feature.Schema;
import com.xx.stdb.base.feature.SimpleFeature;
import com.xx.stdb.engine.hbase.util.EHConstants;
import com.xx.stdb.index.util.GeoTransfer;

/**
 * @author dux(duxionggis@126.com)
 */
public class HbaseFeatureIterator extends FeatureIterator {
	private KryoSerialization kryo;
	private Schema schema;

	private Table table;
	private ResultScanner scanner;

	private Result curResult;
	private int count = 0;

	HbaseFeatureIterator(KryoSerialization kryo, Schema schema, Table table, ResultScanner scanner) {
		this.kryo = kryo;
		this.schema = schema;
		this.table = table;
		this.scanner = scanner;
	}

	@Override
	public boolean hasNext() {
		if (count < EHConstants.LIMIT_SCAN_SIZE) {
			try {
				curResult = scanner.next();
				++count;
				return curResult != null;
			} catch (IOException e) {
				// TODO log
			}
		}
		return false;
	}

	@Override
	public Feature next() {
		if (count <= EHConstants.LIMIT_SCAN_SIZE && curResult != null && !curResult.isEmpty()) {
			byte[] bytes = curResult.getValue(EHConstants.DEF_COL_FAMILY_B, EHConstants.DEF_COL_CELL_B);
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
					f.setFid(EHConstants.getFid(Bytes.toString(curResult.getRow())));
				}
			}
			return f;
		} else {
			return null;
		}
	}

	@Override
	public void remove() {
		// TODO
	}

	@Override
	public void close() {
		scanner.close();
		try {
			table.close();
		} catch (IOException e) {
			// TODO log
		}
	}

}
