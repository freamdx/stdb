package com.xx.stdb.base.feature;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;

import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import com.alibaba.fastjson.JSONObject;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferInputStream;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.ByteBufferOutputStream;
import com.xx.stdb.base.util.Constants;

/**
 * @author dux(duxionggis@126.com)
 */
public class FeatureWrapper {
	private FeatureWrapper() {
	}

	/**
	 * create feature by bytes
	 * 
	 * @param schema
	 *            Schema
	 * @param bytes
	 * @param fidBuffered
	 * @return Feature
	 */
	public static Feature fromBytes(Schema schema, byte[] bytes, boolean fidBuffered) {
		if (schema == null || bytes == null) {
			throw new IllegalArgumentException("schema or bytes is null");
		}

		ByteBufferInput input = new ByteBufferInput(new ByteBufferInputStream(ByteBuffer.wrap(bytes)));
		Feature feature = new Feature(schema);
		int len = input.readInt();
		feature.setGeometryCode(new String(input.readBytes(len), Constants.DEF_CHARSET));
		if (fidBuffered) {
			len = input.readInt();
			feature.setFid(new String(input.readBytes(len), Constants.DEF_CHARSET));
		}
		for (int i = 0; i < schema.getAttributeCount(); i++) {
			if (schema.getAttributeType(i) == AttributeType.BOOLEAN) {
				feature.setAttribute(i, input.readByte() == 1);
			} else if (schema.getAttributeType(i) == AttributeType.INTEGER) {
				feature.setAttribute(i, input.readInt());
			} else if (schema.getAttributeType(i) == AttributeType.FLOAT) {
				feature.setAttribute(i, input.readFloat());
			} else if (schema.getAttributeType(i) == AttributeType.LONG) {
				feature.setAttribute(i, input.readLong());
			} else if (schema.getAttributeType(i) == AttributeType.DATE) {
				feature.setAttribute(i, new Date(input.readLong()));
			} else if (schema.getAttributeType(i) == AttributeType.DOUBLE) {
				feature.setAttribute(i, input.readDouble());
			} else {
				len = input.readInt();
				feature.setAttribute(i, new String(input.readBytes(len), Constants.DEF_CHARSET));
			}
		}
		input.close();
		return feature;
	}

	/**
	 * from simple feature to feature
	 * 
	 * @param schema
	 *            Schema
	 * @param simpleFeature
	 *            SimpleFeature
	 * @return Feature
	 */
	public static Feature fromSimple(Schema schema, SimpleFeature simpleFeature) {
		if (schema == null || schema.getAttributeCount() != simpleFeature.getAttribs().size()) {
			throw new IllegalArgumentException("feature attributes size error");
		}

		Feature feature = new Feature(schema, simpleFeature.getFid());
		List<Schema.Item> items = feature.getSchema().getAttribs();
		JSONObject attribs = simpleFeature.getAttribs();
		for (int i = 0; i < items.size(); i++) {
			feature.setAttribute(i, attribs.getObject(items.get(i).getName(), toClass(items.get(i).getType())));
		}
		try {
			if (simpleFeature.getWkt().endsWith(")")) {
				feature.setGeometry(new WKTReader().read(simpleFeature.getWkt()));
			}
		} catch (ParseException e) {
			throw new IllegalArgumentException(e.getMessage());
		}
		return feature;
	}

	/**
	 * get feature bytes
	 * 
	 * @param feature
	 *            Feature
	 * @param fidBuffered
	 * @return
	 */
	public static byte[] toBytes(Feature feature, boolean fidBuffered) {
		if (feature == null || feature.getGeometryCode() == null || feature.getGeometryCode().isEmpty()) {
			throw new IllegalArgumentException("feature or feature.geometryCode is null");
		}
		ByteBuffer bb = ByteBuffer.allocate(Constants.DEF_BUFFER_SIZE);
		ByteBufferOutput output = new ByteBufferOutput(new ByteBufferOutputStream(bb));
		byte[] byts = feature.getGeometryCode().getBytes(Constants.DEF_CHARSET);
		output.writeInt(byts.length);
		output.writeBytes(byts);
		if (fidBuffered) {
			byts = feature.getFid().getBytes(Constants.DEF_CHARSET);
			output.writeInt(byts.length);
			output.writeBytes(byts);
		}
		for (Object attrib : feature.getAttributes()) {
			if (attrib instanceof Boolean) {
				if ((boolean) attrib) {
					output.writeByte(1);
				} else {
					output.writeByte(0);
				}
			} else if (attrib instanceof Integer) {
				output.writeInt((int) attrib);
			} else if (attrib instanceof Float) {
				output.writeFloat((float) attrib);
			} else if (attrib instanceof Long) {
				output.writeLong((long) attrib);
			} else if (attrib instanceof Date) {
				output.writeLong(((Date) attrib).getTime());
			} else if (attrib instanceof Double) {
				output.writeDouble((double) attrib);
			} else {
				byts = attrib.toString().getBytes(Constants.DEF_CHARSET);
				output.writeInt(byts.length);
				output.writeBytes(byts);
			}
		}

		byte[] bytes = output.toBytes();
		output.close();
		return bytes;
	}

	private static Class<?> toClass(AttributeType type) {
		if (type == AttributeType.BOOLEAN) {
			return Boolean.class;
		} else if (type == AttributeType.INTEGER) {
			return Integer.class;
		} else if (type == AttributeType.FLOAT) {
			return Float.class;
		} else if (type == AttributeType.LONG) {
			return Long.class;
		} else if (type == AttributeType.DOUBLE) {
			return Double.class;
		} else if (type == AttributeType.DATE) {
			return Date.class;
		} else {
			return String.class;
		}
	}

	/**
	 * from feature to simple feature
	 * 
	 * @param feature
	 *            Feature
	 * @return SimpleFeature
	 */
	public static SimpleFeature toSimple(Feature feature) {
		if (feature == null || feature.getGeometry() == null) {
			throw new IllegalArgumentException("feature is null");
		}

		SimpleFeature simpleFeature = new SimpleFeature();
		List<Schema.Item> items = feature.getSchema().getAttribs();
		Object[] attribs = feature.getAttributes();
		for (int i = 0; i < items.size(); i++) {
			simpleFeature.addAttribute(items.get(i).getName(), attribs[i]);
		}
		simpleFeature.setWkt(feature.getGeometry().toText());
		simpleFeature.setFid(feature.getFid());
		return simpleFeature;
	}

}
