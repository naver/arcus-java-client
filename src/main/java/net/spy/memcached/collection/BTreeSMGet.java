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

public interface BTreeSMGet<T> {

	public int headerCount = 4;
	
	public String getCommaSeparatedKeys();

	public String getRepresentKey();

	public List<String> getKeyList();

	public String stringify();

	public String getCommand();

	public boolean headerReady(int spaceCount);

	public String getKey();

	public int getFlag();

	public Object getSubkey();

	public int getDataLength();

	public boolean isReverse();

	public boolean hasEflag();

	public void decodeItemHeader(String itemHeader);
}