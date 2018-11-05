package com.xx.stdb.engine.hbase;

import com.xx.stdb.base.KryoSerialization;
import com.xx.stdb.base.feature.Feature;
import com.xx.stdb.base.feature.FeatureIterator;
import com.xx.stdb.base.feature.Schema;
import com.xx.stdb.engine.hbase.util.EHConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

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
			return HbaseDataSearcher.buildFeature(curResult, kryo, schema);
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
