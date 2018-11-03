package com.xx.stdb.io;

import com.xx.stdb.base.feature.FeatureCollection;

import java.util.Properties;

/**
 * @author dux(duxionggis@126.com)
 */
public interface IDataWriter {
	public void write(FeatureCollection fc, Properties properties) throws Exception;
}
