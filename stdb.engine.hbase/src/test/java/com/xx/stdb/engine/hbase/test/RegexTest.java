package com.xx.stdb.engine.hbase.test;

import java.text.ParseException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import com.xx.stdb.engine.hbase.util.RowRegexUtil;
import com.xx.stdb.index.util.STIConstants;

public class RegexTest {
	public static void main(String[] args) throws ParseException {
		Set<String> codes = new HashSet<>();
		codes.add("35f05294989a3d56ji0");
		codes.add("35f05294989d2f5hq9u");
		codes.add("35f05294989sr6j7k8h");
		codes.add("35f05294989s2f9k1d5");
		codes.add("35f05294989n2n6n8v4");
		codes.add("35f05294989s56k2i8s");
		codes.add("35f05294989k2h6g9b0");
		codes.add("35f05294989k0j8d2k1");
		codes.add("35f05294989b1n3m6a0");
		codes.add("35f05294989x2v4m1x3");
		codes.add("35f05294989l1h6d0s3");
		codes.add("35f0529498982v4m1x3");
		codes.add("35f05294989a1h6d0s3");

		Set<String> fids = new HashSet<>();
		fids.add("2");
		fids.add("3");

		Date from = STIConstants.SDF_HIGH.parse("20180103011359001");
		Date to = STIConstants.SDF_HIGH.parse("20180103111401999");

		System.out.println(RowRegexUtil.regex(codes, null, null, null, STIConstants.SDF_MID));
		System.out.println(RowRegexUtil.regex(codes, fids, null, null, STIConstants.SDF_MID));
		System.out.println(RowRegexUtil.regex(codes, null, null, to, STIConstants.SDF_MID));
		System.out.println(RowRegexUtil.regex(codes, null, from, null, STIConstants.SDF_MID));
		System.out.println(RowRegexUtil.regex(codes, null, from, to, STIConstants.SDF_MID));
		System.out.println(RowRegexUtil.regex(codes, fids, from, to, STIConstants.SDF_MID));
	}

}
