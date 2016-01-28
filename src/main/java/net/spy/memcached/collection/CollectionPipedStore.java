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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import net.spy.memcached.CachedData;
import net.spy.memcached.KeyUtil;
import net.spy.memcached.transcoders.Transcoder;

public abstract class CollectionPipedStore<T> extends CollectionObject {

	public static final String PIPE = "pipe";
	public static final int MAX_PIPED_ITEM_COUNT = 500;
	
	protected String key;
	protected boolean createKeyIfNotExists;
	protected Transcoder<T> tc;
	protected int itemCount;

	protected CollectionAttributes attribute;
	
	/* ENABLE_REPLICATION if */
	/* WHCHOI83_MEMCACHED_REPLICA_GROUP if */
	protected int lastOpIndex = 0;

	public void setLastOperatedIndex(int i) {
		this.lastOpIndex = i;
	}

	/* WHCHOI83_MEMCACHED_REPLICA_GROUP end */
	/* ENABLE_REPLICATION end */
	public abstract ByteBuffer getAsciiCommand();
	public abstract ByteBuffer getBinaryCommand();
	
	/**
	 * 
	 */
	public static class ListPipedStore<T> extends CollectionPipedStore<T> {
		
		private static final String COMMAND = "lop insert";
		private Collection<T> list;
		private int index;
		
		public ListPipedStore(String key, int index, Collection<T> list,
				boolean createKeyIfNotExists, CollectionAttributes attr, Transcoder<T> tc) {
			this.key = key;
			this.index = index;
			this.list = list;
			this.createKeyIfNotExists = createKeyIfNotExists;
			this.attribute = attr;
			this.tc = tc;
			this.itemCount = list.size();
		}
		
		public ByteBuffer getAsciiCommand() {
			int capacity = 0;

			// decode values
			/* ENABLE_REPLICATION if */
			/* WHCHOI83_MEMCACHED_REPLICA_GROUP if */
			List<byte[]> encodedList = new ArrayList<byte[]>(list.size());
			/* ENABLE_REPLICATION else */
			/* WHCHOI83_MEMCACHED_REPLICA_GROUP else */
			/*
			Collection<byte[]> encodedList = new ArrayList<byte[]>(list.size());
			*/
			/* WHCHOI83_MEMCACHED_REPLICA_GROUP end */
			/* ENABLE_REPLICATION end */
			CachedData cd = null;
			for (T each : list) {
				cd = tc.encode(each);
				encodedList.add(cd.getData());
			}
			
			// estimate the buffer capacity
			for (byte[] each : encodedList) {
				capacity += KeyUtil.getKeyBytes(key).length;
				capacity += each.length;
				capacity += 64;
			}

			// allocate the buffer
			ByteBuffer bb = ByteBuffer.allocate(capacity);

			// create ascii operation string
			/* ENABLE_REPLICATION if */
			/* WHCHOI83_MEMCACHED_REPLICA_GROUP if */
			int eSize = encodedList.size();
			for (int i = this.lastOpIndex; i < eSize; i++) {
				byte[] each = encodedList.get(i);
				setArguments(bb, COMMAND, key, index, each.length,
						(createKeyIfNotExists) ? "create" : "", (createKeyIfNotExists) ? cd.getFlags() : "",
						(createKeyIfNotExists) ? (attribute != null && attribute.getExpireTime() != null) ? attribute.getExpireTime() : CollectionAttributes.DEFAULT_EXPIRETIME : "",
						(createKeyIfNotExists) ? (attribute != null && attribute.getMaxCount() != null) ? attribute.getMaxCount() : CollectionAttributes.DEFAULT_MAXCOUNT : "",
						(i < eSize - 1) ? PIPE : "");
				bb.put(each);
				bb.put(CRLF);
			}
			/* ENABLE_REPLICATION else */
			/* WHCHOI83_MEMCACHED_REPLICA_GROUP else */
			/*
			Iterator<byte[]> iterator = encodedList.iterator();
			while (iterator.hasNext()) {
				byte[] each = iterator.next();
				setArguments(bb, COMMAND, key, index, each.length,
						(createKeyIfNotExists) ? "create" : "", (createKeyIfNotExists) ? cd.getFlags() : "",
						(createKeyIfNotExists) ? (attribute != null && attribute.getExpireTime() != null) ? attribute.getExpireTime() : CollectionAttributes.DEFAULT_EXPIRETIME : "",
						(createKeyIfNotExists) ? (attribute != null && attribute.getMaxCount() != null) ? attribute.getMaxCount() : CollectionAttributes.DEFAULT_MAXCOUNT : "",
						(iterator.hasNext()) ? PIPE : "");
				bb.put(each);
				bb.put(CRLF);
			}
			*/
			/* WHCHOI83_MEMCACHED_REPLICA_GROUP end */
			/* ENABLE_REPLICATION end */

			// flip the buffer
			bb.flip();
			
			return bb;
		}

		public ByteBuffer getBinaryCommand() {
			throw new RuntimeException("not supported in binary protocol yet.");
		}
	}
	
	/**
	 * 
	 */
	public static class SetPipedStore<T> extends CollectionPipedStore<T> {
		
		private static final String COMMAND = "sop insert";
		private Collection<T> set;

		public SetPipedStore(String key, Collection<T> set, boolean createKeyIfNotExists,
				CollectionAttributes attr, Transcoder<T> tc) {
			this.key = key;
			this.set = set;
			this.createKeyIfNotExists = createKeyIfNotExists;
			this.attribute = attr;
			this.tc = tc;
			this.itemCount = set.size();
		}
		
		public ByteBuffer getAsciiCommand() {
			int capacity = 0;

			// decode values
			/* ENABLE_REPLICATION if */
			/* WHCHOI83_MEMCACHED_REPLICA_GROUP if */
			List<byte[]> encodedList = new ArrayList<byte[]>(set.size());
			/* ENABLE_REPLICATION else */
			/* WHCHOI83_MEMCACHED_REPLICA_GROUP else */
			/*
			Collection<byte[]> encodedList = new ArrayList<byte[]>(set.size());
			*/
			/* WHCHOI83_MEMCACHED_REPLICA_GROUP end */
			/* ENABLE_REPLICATION end */
			CachedData cd = null;
			for (T each : set) {
				cd = tc.encode(each);
				encodedList.add(cd.getData());
			}
			
			// estimate the buffer capacity
			for (byte[] each : encodedList) {
				capacity += KeyUtil.getKeyBytes(key).length;
				capacity += each.length;
				capacity += 64;
			}

			// allocate the buffer
			ByteBuffer bb = ByteBuffer.allocate(capacity);

			// create ascii operation string
			/* ENABLE_REPLICATION if */
			/* WHCHOI83_MEMCACHED_REPLICA_GROUP if */
			int eSize = encodedList.size();
			for (int i = this.lastOpIndex; i < eSize; i++) {
				byte[] each = encodedList.get(i);

				setArguments(bb, COMMAND, key, each.length,
						(createKeyIfNotExists) ? "create" : "", (createKeyIfNotExists) ? cd.getFlags() : "",
						(createKeyIfNotExists) ? (attribute != null && attribute.getExpireTime() != null) ? attribute.getExpireTime() : CollectionAttributes.DEFAULT_EXPIRETIME : "",
						(createKeyIfNotExists) ? (attribute != null && attribute.getMaxCount() != null) ? attribute.getMaxCount() : CollectionAttributes.DEFAULT_MAXCOUNT : "",
						(i < eSize - 1) ? PIPE : "");
				bb.put(each);
				bb.put(CRLF);
			}
			/* ENABLE_REPLICATION else */
			/* WHCHOI83_MEMCACHED_REPLICA_GROUP else */
			/*
			Iterator<byte[]> iterator = encodedList.iterator();
			while (iterator.hasNext()) {
				byte[] each = iterator.next();
				
				setArguments(bb, COMMAND, key, each.length,
						(createKeyIfNotExists) ? "create" : "", (createKeyIfNotExists) ? cd.getFlags() : "",
						(createKeyIfNotExists) ? (attribute != null && attribute.getExpireTime() != null) ? attribute.getExpireTime() : CollectionAttributes.DEFAULT_EXPIRETIME : "",
						(createKeyIfNotExists) ? (attribute != null && attribute.getMaxCount() != null) ? attribute.getMaxCount() : CollectionAttributes.DEFAULT_MAXCOUNT : "",
						(iterator.hasNext()) ? PIPE : "");
				bb.put(each);
				bb.put(CRLF);
			}
			*/
			/* WHCHOI83_MEMCACHED_REPLICA_GROUP end */
			/* ENABLE_REPLICATION end */
			// flip the buffer
			bb.flip();
			
			return bb;
		}

		public ByteBuffer getBinaryCommand() {
			throw new RuntimeException("not supported in binary protocol yet.");
		}
	}

	/**
	 * 
	 */
	public static class BTreePipedStore<T> extends CollectionPipedStore<T> {
		
		private static final String COMMAND = "bop insert";
		private Map<Long, T> map;

		public BTreePipedStore(String key, Map<Long, T> map, boolean createKeyIfNotExists,
				CollectionAttributes attr, Transcoder<T> tc) {
			this.key = key;
			this.map = map;
			this.createKeyIfNotExists = createKeyIfNotExists;
			this.attribute = attr;
			this.tc = tc;
			this.itemCount = map.size();
		}
		
		public ByteBuffer getAsciiCommand() {
			int capacity = 0;

			// decode parameters
			List<byte[]> decodedList = new ArrayList<byte[]>(map.size());
			CachedData cd = null;
			for (T each : map.values()) {
				cd = tc.encode(each);
				decodedList.add(cd.getData());
			}
			
			// estimate the buffer capacity
			int i = 0;
			for (Long eachBkey : map.keySet()) {
				capacity += KeyUtil.getKeyBytes(key).length;
				capacity += KeyUtil.getKeyBytes(String.valueOf(eachBkey)).length;
				capacity += decodedList.get(i++).length;
				capacity += 64;
			}

			// allocate the buffer
			ByteBuffer bb = ByteBuffer.allocate(capacity);

			// create ascii operation string
			/* ENABLE_REPLICATION if */
			/* WHCHOI83_MEMCACHED_REPLICA_GROUP if */
			int keySize = map.keySet().size();
			List<Long> keyList = new ArrayList<Long>(map.keySet());
			for (i = this.lastOpIndex; i < keySize; i++) {
				Long bkey = keyList.get(i);
				byte[] value = decodedList.get(i);

				setArguments(bb, COMMAND, key, bkey, value.length,
						(createKeyIfNotExists) ? "create" : "", (createKeyIfNotExists) ? cd.getFlags() : "",
						(createKeyIfNotExists) ? (attribute != null && attribute.getExpireTime() != null) ? attribute.getExpireTime() : CollectionAttributes.DEFAULT_EXPIRETIME : "",
						(createKeyIfNotExists) ? (attribute != null && attribute.getMaxCount() != null) ? attribute.getMaxCount() : CollectionAttributes.DEFAULT_MAXCOUNT : "",
						(i < keySize - 1) ? PIPE : "");
				bb.put(value);
				bb.put(CRLF);
			}
			/* ENABLE_REPLICATION else */
			/* WHCHOI83_MEMCACHED_REPLICA_GROUP else */
			/*
			i = 0;
			Iterator<Long> iterator = map.keySet().iterator();
			while (iterator.hasNext()) {
				Long bkey = iterator.next();
				byte[] value = decodedList.get(i++);

				setArguments(bb, COMMAND, key, bkey, value.length,
						(createKeyIfNotExists) ? "create" : "", (createKeyIfNotExists) ? cd.getFlags() : "",
						(createKeyIfNotExists) ? (attribute != null && attribute.getExpireTime() != null) ? attribute.getExpireTime() : CollectionAttributes.DEFAULT_EXPIRETIME : "",
						(createKeyIfNotExists) ? (attribute != null && attribute.getMaxCount() != null) ? attribute.getMaxCount() : CollectionAttributes.DEFAULT_MAXCOUNT : "",
						(iterator.hasNext()) ? PIPE : "");
				bb.put(value);
				bb.put(CRLF);
			}
			*/
			/* WHCHOI83_MEMCACHED_REPLICA_GROUP end */
			/* ENABLE_REPLICATION end */

			// flip the buffer
			bb.flip();
			
			return bb;
		}

		public ByteBuffer getBinaryCommand() {
			throw new RuntimeException("not supported in binary protocol yet.");
		}
	}

	/**
	 * 
	 */
	public static class ByteArraysBTreePipedStore<T> extends
			CollectionPipedStore<T> {

		private static final String COMMAND = "bop insert";
		private List<Element<T>> elements;

		public ByteArraysBTreePipedStore(String key, List<Element<T>> elements,
				boolean createKeyIfNotExists, CollectionAttributes attr,
				Transcoder<T> tc) {
			this.key = key;
			this.elements = elements;
			this.createKeyIfNotExists = createKeyIfNotExists;
			this.attribute = attr;
			this.tc = tc;
			this.itemCount = elements.size();
		}

		public ByteBuffer getAsciiCommand() {
			int capacity = 0;

			// decode parameters
			List<byte[]> decodedList = new ArrayList<byte[]>(elements.size());
			CachedData cd = null;
			for (Element<T> each : elements) {
				cd = tc.encode(each.getValue());
				decodedList.add(cd.getData());
			}

			// estimate the buffer capacity
			int i = 0;
			for (Element<T> each : elements) {
				capacity += KeyUtil.getKeyBytes(key).length;
				capacity += KeyUtil.getKeyBytes((each.isByteArraysBkey() ? each
						.getBkeyByHex() : String.valueOf(each.getLongBkey()))).length;
				capacity += KeyUtil.getKeyBytes(each.getFlagByHex()).length;
				capacity += decodedList.get(i++).length;
				capacity += 64;
			}

			// allocate the buffer
			ByteBuffer bb = ByteBuffer.allocate(capacity);

			// create ascii operation string
			/* ENABLE_REPLICATION if */
			/* WHCHOI83_MEMCACHED_REPLICA_GROUP if */
			int eSize = elements.size();
			for (i = this.lastOpIndex; i < eSize; i++) {
				Element<T> element = elements.get(i);
				byte[] value = decodedList.get(i);

				setArguments(
						bb,
						COMMAND,
						key,
						(element.isByteArraysBkey() ? element.getBkeyByHex()
								: String.valueOf(element.getLongBkey())),
						element.getFlagByHex(),
						value.length,
						(createKeyIfNotExists) ? "create" : "",
						(createKeyIfNotExists) ? cd.getFlags() : "",
						(createKeyIfNotExists) ? (attribute != null && attribute
								.getExpireTime() != null) ? attribute
								.getExpireTime()
								: CollectionAttributes.DEFAULT_EXPIRETIME : "",
						(createKeyIfNotExists) ? (attribute != null && attribute
								.getMaxCount() != null) ? attribute
								.getMaxCount()
								: CollectionAttributes.DEFAULT_MAXCOUNT : "",
						(createKeyIfNotExists) ? (attribute != null && attribute
								.getOverflowAction() != null) ? attribute
								.getOverflowAction().toString()
								: "" : "",
						(createKeyIfNotExists) ? (attribute != null && attribute
								.getReadable() != null && !attribute.getReadable()) ?
								"unreadable" : "" : "",
						(i < eSize - 1) ? PIPE : "");
				bb.put(value);
				bb.put(CRLF);
			}
			/* ENABLE_REPLICATION else */
			/* WHCHOI83_MEMCACHED_REPLICA_GROUP else */
			/*
			i = 0;
			Iterator<Element<T>> iterator = elements.iterator();
			while (iterator.hasNext()) {
				Element<T> element = iterator.next();
				byte[] value = decodedList.get(i++);

				setArguments(
						bb,
						COMMAND,
						key,
						(element.isByteArraysBkey() ? element.getBkeyByHex()
								: String.valueOf(element.getLongBkey())),
						element.getFlagByHex(),
						value.length,
						(createKeyIfNotExists) ? "create" : "",
						(createKeyIfNotExists) ? cd.getFlags() : "",
						(createKeyIfNotExists) ? (attribute != null && attribute
								.getExpireTime() != null) ? attribute
								.getExpireTime()
								: CollectionAttributes.DEFAULT_EXPIRETIME : "",
						(createKeyIfNotExists) ? (attribute != null && attribute
								.getMaxCount() != null) ? attribute
								.getMaxCount()
								: CollectionAttributes.DEFAULT_MAXCOUNT : "",
						(createKeyIfNotExists) ? (attribute != null && attribute
								.getOverflowAction() != null) ? attribute
								.getOverflowAction().toString()
								: "" : "",
						(createKeyIfNotExists) ? (attribute != null && attribute
								.getReadable() != null && !attribute.getReadable()) ?
								"unreadable" : "" : "",
						(iterator.hasNext()) ? PIPE : "");
				bb.put(value);
				bb.put(CRLF);
			}
			*/
			/* WHCHOI83_MEMCACHED_REPLICA_GROUP end */
			/* ENABLE_REPLICATION end */

			// flip the buffer
			bb.flip();

			return bb;
		}

		public ByteBuffer getBinaryCommand() {
			throw new RuntimeException("not supported in binary protocol yet.");
		}
	}
	
	public String getKey() {
		return key;
	}
	
	public void setKey(String key) {
		this.key = key;
	}
	
	public boolean iscreateKeyIfNotExists() {
		return createKeyIfNotExists;
	}
	
	public void setcreateKeyIfNotExists(boolean createKeyIfNotExists) {
		this.createKeyIfNotExists = createKeyIfNotExists;
	}

	public int getItemCount() {
		return this.itemCount;
	}
}
