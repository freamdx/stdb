package com.xx.stdb.io;

import com.xx.stdb.base.feature.FeatureCollection;

import java.util.Properties;

/**
 * @author dux(duxionggis@126.com)
 */
public interface IDataReader {
	public FeatureCollection read(Properties properties) throws Exception;
}
