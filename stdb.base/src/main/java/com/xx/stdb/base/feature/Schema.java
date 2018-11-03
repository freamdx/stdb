package com.xx.stdb.base.feature;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;

/**
 * @author dux(duxionggis@126.com)
 */
public class Schema {
	private List<Item> attribs;

	public Schema() {
		attribs = new ArrayList<>();
	}

	@JSONField(serialize = false)
	public int getAttributeCount() {
		return attribs.size();
	}

	public int getAttributeIndex(String attributeName) {
		int index = -1;
		for (int i = 0; i < attribs.size(); i++) {
			if (attribs.get(i).getName().equals(attributeName)) {
				index = i;
				break;
			}
		}
		if (index < 0) {
			throw new IllegalArgumentException("Unrecognized attribute name: " + attributeName);
		}
		return index;
	}

	public boolean hasAttribute(String attributeName) {
		return attribs.indexOf(attributeName) >= 0;
	}

	public String getAttributeName(int attributeIndex) {
		return attribs.get(attributeIndex).getName();
	}

	public AttributeType getAttributeType(int attributeIndex) {
		return attribs.get(attributeIndex).getType();
	}

	public AttributeType getAttributeType(String attributeName) {
		return getAttributeType(this.getAttributeIndex(attributeName));
	}

	public void addAttribute(String attributeName, AttributeType attributeType) {
		attribs.add(new Item(attributeName, attributeType, false));
	}

	public void addAttribute(String attributeName, AttributeType attributeType, boolean attributeIndex) {
		attribs.add(new Item(attributeName, attributeType, attributeIndex));
	}

	public List<Item> getAttribs() {
		return attribs;
	}

	public String toString() {
		return JSON.toJSONString(this);
	}

	public static Schema fromJson(String json) {
		return JSON.parseObject(json, Schema.class, com.alibaba.fastjson.parser.Feature.OrderedField);
	}

	public static class Item implements Serializable {
		private static final long serialVersionUID = 1L;

		private String name;
		private AttributeType type;
		private boolean indexed;

		public Item() {
		}

		public Item(String name, AttributeType type, boolean indexed) {
			this.name = name;
			this.type = type;
			this.indexed = indexed;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public AttributeType getType() {
			return type;
		}

		public void setType(AttributeType type) {
			this.type = type;
		}

		public boolean isIndexed() {
			return indexed;
		}

		public void setIndexed(boolean indexed) {
			this.indexed = indexed;
		}

	}

}
