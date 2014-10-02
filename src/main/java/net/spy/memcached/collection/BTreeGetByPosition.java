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
 * Ascii protocol implementation for "bop gbp" (B+Tree get by position)
 * 
 * bop gbp <key> <order> <position or "position range">\r\n
 * VALUE <flags> <count>\r\n
 * <bkey> [<eflag>] <bytes> <data>\r\n
 * END\r\n (CLIENT_ERROR, NOT_FOUND, UNREADABLE, TYPE_MISMATCH, NOT_FOUND_ELEMENT)
 */
public class BTreeGetByPosition<T> extends CollectionGet<T> {

	public static final int HEADER_EFLAG_POSITION = 1; // 0-based
	
	private static final String command = "bop gbp";

	private final BTreeOrder order;
	private final String range;
	private final int posFrom;
	private final int posTo;
	
	private BKeyObject bkey;
	private byte[] eflag;
	private int bytes;
	
	public BTreeGetByPosition(BTreeOrder order, int pos) {
		this.headerCount = 2;
		this.order = order;
		this.range = String.valueOf(pos);
		this.posFrom = pos;
		this.posTo = pos;
	}
	
	public BTreeGetByPosition(BTreeOrder order, int posFrom, int posTo) {
		this.headerCount = 2;
		this.order = order;
		this.range = String.valueOf(posFrom) + ".." + String.valueOf(posTo);
		this.posFrom = posFrom;
		this.posTo = posTo;
	}

	public BTreeOrder getOrder() {
		return order;
	}

	public String getRange() {
		return range;
	}

	public String stringify() {
		if (str != null) return str;
		StringBuilder b = new StringBuilder();
		b.append(order.getAscii());
		b.append(" ");
		b.append(range);
		
		str = b.toString();
		return str;
	}

	public String getCommand() {
		return command;
	}

	@Override
	public boolean headerReady(int spaceCount) {
		return spaceCount == 2;
	}

	private static final int BKEY = 0;
	private static final int EFLAG_OR_BYTES = 1;
	private static final int BYTES = 2;
	
	/*
	 * VALUE <flags> <count>\r\n
	 * <bkey> [<eflag>] <bytes> <data>\r\n
	 * END\r\n
	 */
	public void decodeItemHeader(String itemHeader) {
		String[] splited = itemHeader.split(" ");
		boolean hasEFlag = false;

		// <bkey>
		if (splited[BKEY].startsWith("0x")) {
			this.bkey = new BKeyObject(splited[0].substring(2));
		} else {
			this.bkey = new BKeyObject(Long.parseLong(splited[0]));
		}

		// <eflag> or <bytes>
		if (splited[EFLAG_OR_BYTES].startsWith("0x")) {
			// <eflag>
			hasEFlag = true;
			this.eflag = BTreeUtil
					.hexStringToByteArrays(splited[EFLAG_OR_BYTES].substring(2));
		} else {
			this.bytes = Integer.parseInt(splited[EFLAG_OR_BYTES]);
		}
		
		// <bytes>
		if (hasEFlag) {
			this.bytes = Integer.parseInt(splited[BYTES]);
		}
		
		this.dataLength = bytes;
	}

	public BKeyObject getBkey() {
		return bkey;
	}

	public byte[] getEflag() {
		return eflag;
	}

	public int getBytes() {
		return bytes;
	}

	public int getPosFrom() {
		return posFrom;
	}

	public int getPosTo() {
		return posTo;
	}
	
	public boolean isReversed() {
		return posFrom > posTo;
	}

}
