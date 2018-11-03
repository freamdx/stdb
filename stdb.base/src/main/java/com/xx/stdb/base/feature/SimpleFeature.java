package com.xx.stdb.base.feature;

import java.io.Serializable;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * @author dux(duxionggis@126.com)
 */
public class SimpleFeature implements Serializable {
	private static final long serialVersionUID = 1L;

	private String fid;
	private JSONObject attribs;
	private String wkt;

	public SimpleFeature() {
		fid = "";
		attribs = new JSONObject(true);
		wkt = "";
	}

	public String getFid() {
		return fid;
	}

	public void setFid(String fid) {
		this.fid = fid;
	}

	public JSONObject getAttribs() {
		return attribs;
	}

	public void setAttribs(JSONObject attributes) {
		this.attribs = attributes;
	}

	public void addAttribute(String attributeName, Object object) {
		this.attribs.put(attributeName, object);
	}

	public String getWkt() {
		return wkt;
	}

	public void setWkt(String wkt) {
		this.wkt = wkt;
	}

	public String toString() {
		return JSON.toJSONString(this);
	}

	public static SimpleFeature fromJson(String json) {
		return JSON.parseObject(json, SimpleFeature.class, com.alibaba.fastjson.parser.Feature.OrderedField);
	}

}
