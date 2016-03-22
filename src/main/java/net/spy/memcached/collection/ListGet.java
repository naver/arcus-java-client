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

public class ListGet extends CollectionGet {

	public static final int FIRST = 0;
	public static final int LAST = -1;
	
	private static final String command = "lop get";
	
	public ListGet(int index, boolean delete) {
		this.headerCount = 1;
		this.range = String.valueOf(index);
		this.delete = delete;
	}
	
	public ListGet(int index, boolean delete, boolean dropIfEmpty) {
		this(index, delete);
		this.dropIfEmpty = dropIfEmpty;
	}
	
	public ListGet(int from, int to, boolean delete) {
		this.headerCount = 1;
		this.range = String.valueOf(from) + ".." + String.valueOf(to);
		this.delete = delete;
	}

	public ListGet(int from, int to, boolean delete, boolean dropIfEmpty) {
		this(from, to, delete);
		this.dropIfEmpty = dropIfEmpty;
	}
	
	public String getRange() {
		return range;
	}

	public void setRange(String range) {
		this.range = range;
	}

	public String stringify() {
		if (str != null) return str;
		
		StringBuilder b = new StringBuilder();
		b.append(range);
		if (delete && dropIfEmpty) b.append(" drop");
		if (delete && !dropIfEmpty) b.append(" delete");
		
		str = b.toString();
		return str;
	}
	
	public String getCommand() {
		return command;
	}
	
	public void decodeItemHeader(String itemHeader) {
		this.dataLength = Integer.parseInt(itemHeader);
	}
}
