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

public class MapDelete extends CollectionDelete {
	private static final String command = "mop delete";

	protected List<String> mkeyList;
	private String commaSeparatedKeys;
	protected byte[] additionalArgs;

	public MapDelete(List<String> mkeyList, boolean noreply) {
		this.mkeyList = mkeyList;
		this.noreply = noreply;
		if (mkeyList.size() == 0) {
			this.additionalArgs = null;
		} else {
			this.additionalArgs = getCommaSeparatedMkeys().getBytes();
		}
	}

	public MapDelete(List<String> mkeyList, boolean noreply, boolean dropIfEmpty) {
		this(mkeyList, noreply);
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

	public byte[] getAdditionalArgs() {
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

		if (dropIfEmpty) {
			b.append(" drop");
		}
		if (noreply) {
			b.append(" noreply");
		}

		str = b.toString();
		return str;
	}

	public String getCommand() {
		return command;
	}
}
