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
import java.util.Iterator;
import java.util.List;

import net.spy.memcached.CachedData;
import net.spy.memcached.KeyUtil;
import net.spy.memcached.collection.ElementFlagFilter.BitWiseOperands;
import net.spy.memcached.transcoders.Transcoder;

public abstract class CollectionPipedUpdate<T> extends CollectionObject {

	public static final String PIPE = "pipe";
	public static final int MAX_PIPED_ITEM_COUNT = 500;

	protected String key;
	protected Transcoder<T> tc;
	protected int itemCount;

	public abstract ByteBuffer getAsciiCommand();

	public abstract ByteBuffer getBinaryCommand();

	public static class BTreePipedUpdate<T> extends CollectionPipedUpdate<T> {

		private static final String COMMAND = "bop update";
		private List<Element<T>> elements;

		public BTreePipedUpdate(String key, List<Element<T>> elements,
				Transcoder<T> tc) {
			this.key = key;
			this.elements = elements;
			this.tc = tc;
			this.itemCount = elements.size();
		}

		public ByteBuffer getAsciiCommand() {
			int capacity = 0;

			// decode parameters
			List<byte[]> decodedList = new ArrayList<byte[]>(elements.size());
			CachedData cd = null;
			for (Element<T> each : elements) {
				if (each.getValue() != null) {
					cd = tc.encode(each.getValue());
					decodedList.add(cd.getData());
				} else {
					decodedList.add(null);
				}
			}

			// estimate the buffer capacity
			int i = 0;
			ElementFlagUpdate elementFlagUpdate;
			byte[] elementFlag;
			int flagOffset;
			BitWiseOperands bitOp;
			byte[] value;
			StringBuilder b;

			for (Element<T> each : elements) {
				elementFlagUpdate = each.getElementFlagUpdate();
				if (elementFlagUpdate != null) {
					// eflag
					elementFlag = elementFlagUpdate.getElementFlag();
					if (elementFlag != null) {
						capacity += KeyUtil.getKeyBytes(elementFlagUpdate
								.getElementFlagByHex()).length;

						// fwhere bitwop
						if (elementFlagUpdate.getElementFlagOffset() > -1) {
							capacity += 6;
						}
					}
				}

				capacity += KeyUtil.getKeyBytes(key).length;
				capacity += KeyUtil.getKeyBytes((each.isByteArraysBkey() ? each
						.getBkeyByHex() : String.valueOf(each.getLongBkey()))).length;
				if (decodedList.get(i) != null) {
					capacity += decodedList.get(i++).length;
				}
				capacity += 64;
			}

			// allocate the buffer
			ByteBuffer bb = ByteBuffer.allocate(capacity);

			// create ascii operation string
			i = 0;

			Iterator<Element<T>> iterator = elements.iterator();
			while (iterator.hasNext()) {
				Element<T> element = iterator.next();

				flagOffset = -1;
				bitOp = null;
				elementFlag = null;
				value = decodedList.get(i++);
				elementFlagUpdate = element.getElementFlagUpdate();
				b = new StringBuilder();

				// has element eflag update
				if (elementFlagUpdate != null) {
					elementFlag = elementFlagUpdate.getElementFlag();
					if (elementFlag != null) {
						if (elementFlag.length > 0) {
							// use fwhere bitop
							flagOffset = elementFlagUpdate.getElementFlagOffset();
							bitOp = elementFlagUpdate.getBitOp();
							if (flagOffset > -1 && bitOp != null) {
								b.append(flagOffset).append(" ").append(bitOp)
										.append(" ");
							}

							b.append(elementFlagUpdate.getElementFlagByHex());
						} else {
							b.append("0");
						}
					}
				}

				setArguments(bb, COMMAND, key,
						(element.isByteArraysBkey() ? element.getBkeyByHex()
								: String.valueOf(element.getLongBkey())),
						b.toString(), (value == null ? -1 : value.length),
						(iterator.hasNext()) ? PIPE : "");
				if (value != null) {
					if (value.length > 0) {
						bb.put(value);
					}
					bb.put(CRLF);
				}
			}

			// flip the buffer
			bb.flip();

			return bb;
		}

		public ByteBuffer getBinaryCommand() {
			throw new RuntimeException("not supported in binary protocol yet.");
		}
	}

	public static class MapPipedUpdate<T> extends CollectionPipedUpdate<T> {

		private static final String COMMAND = "mop update";
		private List<MapField<T>> mapFields;

		public MapPipedUpdate(String key, List<MapField<T>> mapFields,
								Transcoder<T> tc) {
			this.key = key;
			this.mapFields = mapFields;
			this.tc = tc;
			this.itemCount = mapFields.size();
		}

		public ByteBuffer getAsciiCommand() {
			int capacity = 0;

			// decode parameters
			List<byte[]> decodedList = new ArrayList<byte[]>(mapFields.size());
			CachedData cd = null;
			for (MapField<T> each : mapFields) {
				if (each.getValue() != null) {
					cd = tc.encode(each.getValue());
					decodedList.add(cd.getData());
				} else {
					decodedList.add(null);
				}
			}

			// estimate the buffer capacity
			int i = 0;
			byte[] value;
			StringBuilder b;

			for (MapField<T> each : mapFields) {
				capacity += KeyUtil.getKeyBytes(key).length;
				capacity += KeyUtil.getKeyBytes(String.valueOf(each.getField())).length;
				if (decodedList.get(i) != null) {
					capacity += decodedList.get(i++).length;
				}
				capacity += 64;
			}

			// allocate the buffer
			ByteBuffer bb = ByteBuffer.allocate(capacity);

			// create ascii operation string
			i = 0;

			Iterator<MapField<T>> iterator = mapFields.iterator();
			while (iterator.hasNext()) {
				MapField<T> mapField = iterator.next();
				value = decodedList.get(i++);
				b = new StringBuilder();

				setArguments(bb, COMMAND, key,
						String.valueOf(mapField.getField()),
						b.toString(), (value == null ? -1 : value.length),
						(iterator.hasNext()) ? PIPE : "");
				if (value != null) {
					if (value.length > 0) {
						bb.put(value);
					}
					bb.put(CRLF);
				}
			}

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

	public int getItemCount() {
		return this.itemCount;
	}
}
