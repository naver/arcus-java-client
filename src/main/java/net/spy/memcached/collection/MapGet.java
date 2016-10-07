/*
 * arcus-java-client : Arcus Java client
 * Copyright 2016 JaM2in Co., Ltd.
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

public class MapGet extends CollectionGet {

	private static final String command = "mop get";

	protected List<String> mkeyList;
	protected byte[] data;
	private String commaSeparatedKeys;
	protected byte[] additionalArgs;

	public MapGet(List<String> mkeyList, boolean delete) {
		this.headerCount = 2;
		this.mkeyList = mkeyList;
		this.delete = delete;
		if (mkeyList.size() == 0) {
			this.additionalArgs = null;
		} else {
			this.additionalArgs = getCommaSeparatedMkeys().getBytes();
		}
	}

	public MapGet(List<String> mkeyList, boolean delete, boolean dropIfEmpty) {
		this(mkeyList, delete);
		this.dropIfEmpty = dropIfEmpty;
	}

	public String getCommaSeparatedMkeys() {
		if (commaSeparatedKeys != null) {
			return commaSeparatedKeys;
		}

		StringBuilder sb = new StringBuilder();
		int numkeys = mkeyList.size();
		for (int i = 0; i < numkeys; i++) {
			sb.append(mkeyList.get(i));
			if ((i + 1) < numkeys) {
				sb.append(",");
			}
		}
		commaSeparatedKeys = sb.toString();
		return commaSeparatedKeys;
	}

	@Override
	public byte[] getAddtionalArgs() {
		return additionalArgs;
	}

	public String stringify() {
		if (str != null) return str;

		StringBuilder b = new StringBuilder();
		if (additionalArgs == null) {
			b.append("0");
		} else {
			b.append(additionalArgs.length);
		}
		b.append(" ").append(mkeyList.size());

		if (delete && dropIfEmpty) {
			b.append(" drop");
		}
		if (delete && !dropIfEmpty) {
			b.append(" delete");
		}

		str = b.toString();
		return str;
	}

	public String getCommand() {
		return command;
	}

	public void decodeItemHeader(String itemHeader) {
		String[] splited = itemHeader.split(" ");
		this.subkey = splited[0];
		this.dataLength = Integer.parseInt(splited[1]);
	}
}
