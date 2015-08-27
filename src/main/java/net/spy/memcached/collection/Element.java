/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.spy.memcached.collection;

import net.spy.memcached.util.BTreeUtil;

/**
 * Collection element
 * 
 * @param <T>
 */
public class Element<T> {
	private final byte[] bkey;
	private final Long longBkey;
	private final T value;
	private final byte[] eflag;
	private final ElementFlagUpdate elementFlagUpdate;

	private final boolean isByteArraysBkey;
	
	/**
	 * Create an element 
	 * 
	 * @param bkey key of element
	 * @param value value of element
	 * @param eflag flag of element (minimun length is 1. maximum length is 31)
	 */
	public Element(byte[] bkey, T value, byte[] eflag) {
		this.bkey = bkey;
		this.longBkey = null;
		this.value = value;
		this.eflag = eflag;
		this.isByteArraysBkey = true;
		this.elementFlagUpdate = null;
	}
	
	public Element(long bkey, T value, byte[] eflag) {
		this.bkey = null;
		this.longBkey = bkey;
		this.value = value;
		this.eflag = eflag;
		this.isByteArraysBkey = false;
		this.elementFlagUpdate = null;
	}

	public Element(byte[] bkey, T value, ElementFlagUpdate elementFlagUpdate) {
		this.bkey = bkey;
		this.longBkey = null;
		this.value = value;
		this.eflag = null;
		this.isByteArraysBkey = true;
		this.elementFlagUpdate = elementFlagUpdate;
	}

	public Element(long bkey, T value, ElementFlagUpdate elementFlagUpdate) {
		this.bkey = null;
		this.longBkey = bkey;
		this.value = value;
		this.eflag = null;
		this.isByteArraysBkey = false;
		this.elementFlagUpdate = elementFlagUpdate;
	}

	/**
	 * get value of element flag by hex.
	 * 
	 * @return element flag by hex (e.g. 0x01)
	 */
	public String getFlagByHex() {
		// convert to hex based on its real byte array
		if (eflag == null) {
			return "";
		}

		if (eflag.length == 0) {
			return "0";
		}

		return BTreeUtil.toHex(eflag);
	}

	/**
	 * get bkey
	 * 
	 * @return bkey by hex (e.g. 0x01)
	 */
	public String getBkeyByHex() {
		return BTreeUtil.toHex(bkey);
	}
	
	/**
	 * get bkey
	 * 
	 * @return bkey by byte[]
	 */
	public byte[] getByteArrayBkey() {
		return bkey;
	}

	/**
	 * get bkey
	 * 
	 * @return bkey (-1 if not available)
	 */
	public long getLongBkey() {
		return (longBkey == null)? -1 : longBkey;
	}

	/**
	 * get value
	 * 
	 * @return value
	 */
	public T getValue() {
		return value;
	}

	/**
	 * get flag
	 * 
	 * @return element flag
	 */
	public byte[] getFlag() {
		return eflag;
	}

	public boolean isByteArraysBkey() {
		return isByteArraysBkey;
	}

	public ElementFlagUpdate getElementFlagUpdate() {
		return elementFlagUpdate;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("{ \"");
		if (isByteArraysBkey) {
			sb.append(getBkeyByHex());
		} else {
			sb.append(getLongBkey());
		}
		sb.append("\" : { ");
		
		sb.append(" \"eflag\" : \"").append(BTreeUtil.toHex(eflag)).append("\"");
		sb.append(",");
		sb.append(" \"value\" : \"").append(value.toString()).append("\"");
		sb.append(" }");
		
		return sb.toString();
	}
	
}
