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

public abstract class BTreeGetBulkImpl<T> implements BTreeGetBulk<T> {

	private static final String command = "bop mget";

	private String commaSeparatedKeys;

	protected String str;
	protected int lenKeys;

	protected List<String> keyList;
	protected String range;
	protected ElementFlagFilter eFlagFilter;
	protected int offset = -1;
	protected int count;
	protected boolean reverse;

	protected Map<Integer, T> map;

	public String key;
	public int flag;
	public Object subkey;
	public int dataLength;
	public byte[] eflag = null;

	protected BTreeGetBulkImpl(List<String> keyList, byte[] from, byte[] to,
			ElementFlagFilter eFlagFilter, int offset, int count) {
		
		this.keyList = keyList;
		this.range = BTreeUtil.toHex(from) + ".." + BTreeUtil.toHex(to);
		this.eFlagFilter = eFlagFilter;
		this.offset = offset;
		this.count = count;
		this.reverse = BTreeUtil.compareByteArraysInLexOrder(from, to) > 0;
	}

	protected BTreeGetBulkImpl(List<String> keyList, long from, long to,
			ElementFlagFilter eFlagFilter, int offset, int count) {
		
		this.keyList = keyList;
		this.range = String.valueOf(from) + ((to > -1) ? ".." + String.valueOf(to) : "");
		this.eFlagFilter = eFlagFilter;
		this.offset = offset;
		this.count = count;
		this.reverse = (from > to);
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

		/*
		 * 
		 * bop mget <lenkeys> <numkeys> <bkey or "bkey range"> [<eflag_filter>]
		 * [<offset>] <count>\r\n<"comma separated keys">\r\n
		 * <eflag_filter> : <fwhere> [<bitwop> <foperand>] <compop> <fvalue>
		 * 
		 */
		
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

	public boolean elementHeaderReady(int spaceCount) {
		return spaceCount == 3 || spaceCount == 4;
	}

	public boolean keyHeaderReady(int spaceCount) {
		return spaceCount == 3 || spaceCount == 5;
	}

	public String getKey() {
		return key;
	}

	public int getFlag() {
		return flag;
	}

	public int getDataLength() {
		return dataLength;
	}

	public byte[] getEFlag() {
		return eflag;
	}
}