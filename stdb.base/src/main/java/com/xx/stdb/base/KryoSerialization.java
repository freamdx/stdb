package com.xx.stdb.base;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * @author dux(duxionggis@126.com)
 */
public class KryoSerialization {
	private Kryo kryo = null;
	private Registration register = null;

	public KryoSerialization() {
		kryo = new Kryo();
		kryo.setReferences(true);
	}

	public void register(Class<?> t) {
		register = kryo.register(t);
	}

	public byte[] serialize(Object object) {
		Output output = new Output(1, 4096);
		kryo.writeObject(output, object);
		byte[] bt = output.toBytes();
		output.flush();
		return bt;
	}

	public <T> T deserialize(byte[] bt) {
		Input input = new Input(bt);
		T res = (T) kryo.readObject(input, register.getType());
		input.close();
		return res;
	}
}