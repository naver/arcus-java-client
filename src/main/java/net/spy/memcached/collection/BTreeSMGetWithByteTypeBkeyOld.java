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

import java.util.List;
import java.util.Map;

import net.spy.memcached.util.BTreeUtil;

public class BTreeSMGetWithByteTypeBkeyOld<T> implements BTreeSMGet<T> {

	private static final String command = "bop smget";

	protected String str;

	protected List<String> keyList;
	private String commaSeparatedKeys;
	
	protected int lenKeys;

	protected String range;
	protected int offset = -1;
	protected int count;
	protected Map<Integer, T> map;

	protected boolean reverse;

	public String key;
	public int flag;
	public byte[] subkey;
	public int dataLength;

	public byte[] eflag = null;
	
	private ElementFlagFilter eFlagFilter;
	
	public BTreeSMGetWithByteTypeBkeyOld(List<String> keyList, byte[] from,
			byte[] to, ElementFlagFilter eFlagFilter, int offset, int count) {
		this.keyList = keyList;
		this.range = BTreeUtil.toHex(from)  + ".." + BTreeUtil.toHex(to);
		this.eFlagFilter = eFlagFilter;
		this.offset = offset;
		this.count = count;
		this.reverse = BTreeUtil.compareByteArraysInLexOrder(from, to) > 0;
	}
	
	public String getCommaSeparatedKeys() {
		if (commaSeparatedKeys != null) {
			return commaSeparatedKeys;
		}
		
		StringBuilder sb = new StringBuilder();
		int numkeys = keyList.size();
		for (int i = 0; i < numkeys; i++) {
			sb.append(keyList.get(i));
			if ((i + 1) < numkeys) {
				sb.append(",");
			}
		}
		commaSeparatedKeys = sb.toString();
		return commaSeparatedKeys;
	}
	
	public String getRepresentKey() {
		if (keyList == null || keyList.isEmpty()) {
			throw new IllegalStateException("Key list is empty.");
		}
		return keyList.get(0);
	}
	
	public List<String> getKeyList() {
		return keyList;
	}

	public String stringify() {
		if (str != null)
			return str;

		StringBuilder b = new StringBuilder();

		b.append(getCommaSeparatedKeys().length());
		b.append(" ").append(keyList.size());
		b.append(" ").append(range);
		
		if (eFlagFilter != null)
			b.append(" ").append(eFlagFilter.toString());
		
		if (offset > 0)
			b.append(" ").append(offset);

		b.append(" ").append(count);

		str = b.toString();
		return str;
	}

	public String getCommand() {
		return command;
	}

	public boolean headerReady(int spaceCount) {
		return headerCount == spaceCount;
	}

	public String getKey() {
		return key;
	}
	
	public int getFlag() {
		return flag;
	}
	
	public byte[] getSubkey() {
		return subkey;
	}

	public int getDataLength() {
		return dataLength;
	}

	public boolean isReverse() {
		return reverse;
	}
	
	public boolean hasEflag() {
		return eflag != null;
	}
	
	public void decodeItemHeader(String itemHeader) {
		String[] splited = itemHeader.split(" ");
		
		/*
		with flag
			VALUE 1
			SMGetTest31 0 0x01 0x45464C4147 6 VALUE1
			MISSED_KEYS 0
			END
		
		without flag
			VALUE 1
			SMGetTest31 0 0x01 6 VALUE1
			MISSED_KEYS 0
			END
		 */
		this.key = splited[0];
		this.flag = Integer.parseInt(splited[1]);
		this.subkey = BTreeUtil.hexStringToByteArrays(splited[2].substring(2));

		if (splited[3].startsWith("0x")) {
			this.eflag = BTreeUtil.hexStringToByteArrays(splited[3].substring(2));
			this.dataLength = Integer.parseInt(splited[4]);
		} else {
			this.eflag = null;
			this.dataLength = Integer.parseInt(splited[3]);
		}
	}
}