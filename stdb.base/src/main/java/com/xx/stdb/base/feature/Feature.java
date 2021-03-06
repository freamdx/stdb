package com.xx.stdb.base.feature;

import java.util.Arrays;
import java.util.Date;

import org.locationtech.jts.geom.Geometry;

/**
 * @author dux(duxionggis@126.com)
 */
public class Feature {
	private String fid;
	private Object[] attributes;
	private Geometry geometry;
	private Schema schema;

	private String geometryCode; // only method(toBytes/fromBytes) used

	public Feature(Schema schema) {
		this.fid = "";
		this.schema = schema;
		attributes = new Object[schema.getAttributeCount()];
	}

	public Feature(Schema schema, String fid) {
		this(schema);
		this.fid = fid;
	}

	public String getFid() {
		return fid;
	}

	public void setFid(String fid) {
		this.fid = fid;
	}

	public Schema getSchema() {
		return schema;
	}

	public void setSchema(Schema schema) {
		this.schema = schema;
	}

	public void setAttribute(String attributeName, Object newAttribute) {
		setAttribute(schema.getAttributeIndex(attributeName), newAttribute);
	}

	public void setAttribute(int attributeIndex, Object newAttribute) {
		attributes[attributeIndex] = newAttribute;
	}

	public Object getAttribute(String attributeName) {
		return getAttribute(schema.getAttributeIndex(attributeName));
	}

	public Object getAttribute(int attributeIndex) {
		return attributes[attributeIndex];
	}

	public Date firstIndexedDateAttrib() {
		Date date = null;
		for (int i = 0; i < schema.getAttribs().size(); i++) {
			Schema.Item item = schema.getAttribs().get(i);
			if (item.isIndexed() && item.getType() == AttributeType.DATE) {
				date = (Date) attributes[i];
				break;
			}
		}
		return date;
	}

	public void setAttributes(Object[] attributes) {
		this.attributes = attributes;
	}

	public Object[] getAttributes() {
		return attributes;
	}

	public String getString(int attributeIndex) {
		Object result = getAttribute(attributeIndex);
		if (result != null) {
			return result.toString();
		} else {
			return "";
		}
	}

	public String getString(String attributeName) {
		Object result = getAttribute(attributeName);
		if (result != null) {
			return result.toString();
		} else {
			return "";
		}
	}

	public Geometry getGeometry() {
		return geometry;
	}

	public void setGeometry(Geometry geometry) {
		this.geometry = geometry;
	}

	public String getGeometryCode() {
		return this.geometryCode;
	}

	public void setGeometryCode(String geometryCode) {
		this.geometryCode = geometryCode;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(attributes);
		result = prime * result + ((fid == null) ? 0 : fid.hashCode());
		result = prime * result + ((geometry == null) ? 0 : geometry.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Feature other = (Feature) obj;
		if (!Arrays.equals(attributes, other.attributes))
			return false;
		if (fid == null) {
			if (other.fid != null)
				return false;
		} else if (!fid.equals(other.fid))
			return false;
		if (geometry == null) {
			if (other.geometry != null)
				return false;
		} else if (!geometry.equals(other.geometry))
			return false;
		return true;
	}

}
