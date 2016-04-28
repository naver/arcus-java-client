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

public abstract class CollectionDelete {

	protected String range;
	protected boolean noreply;
	protected boolean dropIfEmpty = true;
	
	protected String str;

	public String getRange() {
		return range;
	}

	public void setRange(String range) {
		this.range = range;
	}

	public boolean isNoreply() {
		return noreply;
	}

	public void setNoreply(boolean noreply) {
		this.noreply = noreply;
	}

	public abstract byte[] getData();
	public abstract void setData(byte[] data);
	public abstract String stringify();
	public abstract String getCommand();

}
