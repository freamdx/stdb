package com.xx.stdb.engine.hbase.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author dux(duxionggis@126.com)
 */
public class HbaseWrapper {
	public static final Algorithm ALG_COMPRESS = Algorithm.LZ4;
	public static final long FILE_SIZE = 1073741824;

	private Admin admin;
	private Connection conn;

	public HbaseWrapper(String zkQuorum, String zkPort) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", zkQuorum);
		conf.set("hbase.zookeeper.property.clientPort", zkPort);

		conn = ConnectionFactory.createConnection(conf);
		admin = conn.getAdmin();
	}

	public void close() throws IOException {
		admin.close();
		conn.close();
	}

	public boolean existTable(String tableName) throws IOException {
		return admin.tableExists(TableName.valueOf(tableName));
	}

	public Table getTable(String tableName) throws IOException {
		return conn.getTable(TableName.valueOf(tableName));
	}

	public void createTable(String tableName, String... cks) throws IOException {
		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
		addColumn(tableDescriptor, cks);
		tableDescriptor.setMaxFileSize(FILE_SIZE);
		admin.createTable(tableDescriptor);
	}

	public void deleteTable(String tableName) throws IOException {
		TableName tabName = TableName.valueOf(tableName);
		admin.disableTable(tabName);
		admin.deleteTable(tabName);
	}

	public void addFamily(String tableName, String... cks) throws IOException {
		TableName tabName = TableName.valueOf(tableName);
		HTableDescriptor tableDescriptor = admin.getTableDescriptor(tabName);
		addColumn(tableDescriptor, cks);
		admin.disableTable(tabName);
		admin.modifyTable(tabName, tableDescriptor);
		admin.enableTable(tabName);
	}

	public void deleteFamily(String tableName, String... cks) throws IOException {
		TableName tabName = TableName.valueOf(tableName);
		HTableDescriptor tableDescriptor = admin.getTableDescriptor(tabName);
		admin.disableTable(tabName);
		for (int i = 0; i < cks.length; i++) {
			if (tableDescriptor.hasFamily(Bytes.toBytes(cks[i]))) {
				admin.deleteColumn(tabName, Bytes.toBytes(cks[i]));
			}
		}
		admin.enableTable(tabName);
	}

	private void addColumn(HTableDescriptor tableDescriptor, String[] cks) {
		HColumnDescriptor columnDescriptor = null;
		for (int i = 0; i < cks.length; i++) {
			if (!tableDescriptor.hasFamily(Bytes.toBytes(cks[i]))) {
				columnDescriptor = new HColumnDescriptor(Bytes.toBytes(cks[i]));
				columnDescriptor.setCompactionCompressionType(ALG_COMPRESS);
				columnDescriptor.setMaxVersions(Integer.MAX_VALUE);
				tableDescriptor.addFamily(columnDescriptor);
			}
		}
	}

	public void setValue(String tableName, Put put) throws IOException {
		Table hTable = this.getTable(tableName);
		try {
			hTable.put(put);
		} finally {
			hTable.close();
		}
	}

	public Result getValue(String tableName, Get get) throws IOException {
		Result result = null;
		Table hTable = this.getTable(tableName);
		try {
			result = hTable.get(get);
		} finally {
			hTable.close();
		}
		return result;
	}

	public void multiSet(String tableName, List<Put> puts) throws IOException {
		Table hTable = this.getTable(tableName);
		try {
			hTable.put(puts);
		} finally {
			hTable.close();
		}
	}

	public Result[] multiGet(String tableName, List<Get> gets) throws IOException {
		Result[] results = null;
		Table hTable = this.getTable(tableName);
		try {
			results = hTable.get(gets);
		} finally {
			hTable.close();
		}
		return results;
	}

	public void multiDelete(String tableName, List<Delete> deletes) throws IOException {
		Table hTable = this.getTable(tableName);
		try {
			hTable.delete(deletes);
		} finally {
			hTable.close();
		}
	}

	public Result[] scan(String tableName, Scan scan) throws IOException {
		List<Result> results = new ArrayList<>();
		Table hTable = this.getTable(tableName);
		try {
			ResultScanner rs = hTable.getScanner(scan);
			for (Result r : rs) {
				results.add(r);
			}
			rs.close();
		} finally {
			hTable.close();
		}
		return results.isEmpty() ? null : results.toArray(new Result[0]);
	}

}
