
package com.xx.stdb.engine.hbase.test;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

import com.xx.stdb.engine.hbase.util.HbaseWrapper;

public class HbaseTest {

	public static void main(String[] args) throws IOException {
		String tableName = "trace_test_hilbert_mid_v1";
		// String tableName = "trace_tdrive_attr_v5";
		// String tableName = "trace_tdrive_z2_v2";
		// String tableName = "trace_tdrive_z3_v2";

		HbaseWrapper hbase = new HbaseWrapper("localhost", "2181");
		System.out.println(hbase.existTable(tableName));

		// Get
		Get get = new Get(Bytes.toBytes("1277")); // 1277
		get.setMaxVersions();
		Result result = hbase.getValue(tableName, get);
		if (!result.isEmpty()) {
			for (Cell c : result.listCells()) {
				String row = Bytes.toString(c.getRowArray(), c.getRowOffset(), c.getRowLength());
				String family = Bytes.toString(c.getFamilyArray(), c.getFamilyOffset(), c.getFamilyLength());
				String val = Bytes.toString(c.getValueArray(), c.getValueOffset(), c.getValueLength());
				String qualifier = Bytes.toString(c.getQualifierArray(), c.getQualifierOffset(),
						c.getQualifierLength());

				System.out.println("row=>" + row);
				System.out.println("family=>" + family + " : " + val);
				System.out.println("qualifier=>" + qualifier);
				System.out.println();
			}
		}

		// Scan
		Scan scan = new Scan();
		scan.setMaxVersions();
		scan.setRaw(true);
		scan.setCaching(512);
		scan.setBatch(1024);

		FilterList filterList = new FilterList();
		RegexStringComparator comparator = new RegexStringComparator("35f05294989.{4}2018110307[0-9]{4}:.+");
		Filter filter = new RowFilter(CompareOp.EQUAL, comparator);
		filterList.addFilter(filter);
		comparator = new RegexStringComparator("35f0529498.{5}20181103[0-9]{6}:.+");
		filter = new RowFilter(CompareOp.EQUAL, comparator);
		filterList.addFilter(filter);
		scan.setFilter(filterList);

		Result[] rs = hbase.scan(tableName, scan);
		int n = 0;
		for (Result r : rs) {
			if (r.isEmpty()) {
				continue;
			}

			for (Cell c : r.listCells()) {
				if (++n < 50) {
					String row = Bytes.toString(c.getRowArray(), c.getRowOffset(), c.getRowLength());
					String val = Bytes.toString(c.getValueArray(), c.getValueOffset(), c.getValueLength());
					System.out.println(n + " timestamp: " + c.getTimestamp() + " row: " + row + " : " + val);
				} else if (n == 50) {
					System.out.println("...");
				}
			}
		}
		System.out.println("total=>" + n);
	}

}
