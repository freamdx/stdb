package com.xx.stdb.engine;

import java.util.List;
import java.util.Set;

import com.xx.stdb.base.feature.Feature;
import com.xx.stdb.base.feature.FeatureIterator;

/**
 * @author dux(duxionggis@126.com)
 */
public interface IDataSearcher {

	/**
	 * get single feature by feature id
	 * 
	 * @param fid
	 *            String
	 * @return Feature
	 * @throws EngineException
	 */
	Feature get(String fid) throws EngineException;

	/**
	 * get multi features
	 * 
	 * @param fids
	 *            Set<String>
	 * @return List<Feature>
	 * @throws EngineException
	 */
	List<Feature> multiGet(Set<String> fids) throws EngineException;

	/**
	 * get all features
	 * 
	 * @return FeatureIterator
	 * @throws EngineException
	 */
	FeatureIterator readAll() throws EngineException;

	/**
	 * spatio-temporal query
	 * 
	 * @param filter
	 *            STFilter
	 * @return Set<Feature>
	 * @throws EngineException
	 */
	Set<Feature> spatioTemporalQuery(STFilter filter) throws EngineException;

}
