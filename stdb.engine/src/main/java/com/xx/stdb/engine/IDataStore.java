package com.xx.stdb.engine;

import com.xx.stdb.base.feature.Feature;
import com.xx.stdb.base.feature.Schema;
import com.xx.stdb.index.IndexEnums.IndexLevel;
import com.xx.stdb.index.IndexEnums.IndexType;

/**
 * @author dux(duxionggis@126.com)
 */
public interface IDataStore {

	/**
	 * set feature collection name
	 *
	 * @param collectionName
	 *            String
	 */
	void setCollectionName(String collectionName);

	/**
	 * set spatio-temporal indexes
	 *
	 * @param type
	 *            IndexType
	 * @param level
	 *            IndexLevel
	 */
	void addSTIndex(IndexType type, IndexLevel level);

	/**
	 * get feature schema
	 *
	 * @return Schema
	 */
	Schema getSchema() throws EngineException;

	/**
	 * create metadata
	 *
	 * @param schema
	 *            Schema
	 * @throws EngineException
	 */
	void createMetadata(Schema schema) throws EngineException;

	/**
	 * delete metadata
	 *
	 * @throws EngineException
	 */
	void deleteMetadata() throws EngineException;

	/**
	 * write feature
	 *
	 * @param feature
	 *            Feature
	 * @throws EngineException
	 */
	void writeFeature(Feature feature) throws EngineException;

	/**
	 * create searcher
	 * 
	 * @return
	 * @throws EngineException
	 */
	IDataSearcher createSearcher() throws EngineException;

	/**
	 * destroy and release
	 */
	void destroy();

}
