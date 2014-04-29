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

public class ListDelete<T> extends CollectionDelete<T> {

	private static final String command = "lop delete";
	
	public ListDelete(int index, boolean noreply) {
		this.range = String.valueOf(index);
		this.noreply = noreply;
	}

	public ListDelete(int index, boolean noreply, boolean dropIfEmpty) {
		this(index, noreply);
		this.dropIfEmpty = dropIfEmpty;
	}
	
	public ListDelete(int from, int to, boolean noreply) {
		this.range = String.valueOf(from) + ".." + String.valueOf(to);
		this.noreply = noreply;
	}

	public ListDelete(int from, int to, boolean noreply, boolean dropIfEmpty) {
		this(from, to, noreply);
		this.dropIfEmpty = dropIfEmpty;
	}
	
	public String stringify() {
		if (str != null) return str;
		
		StringBuilder b = new StringBuilder();
		b.append(range);
		
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
