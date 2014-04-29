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

public abstract class ElementValueType {

	public static final ElementValueType STRING = new StringType();
	public static final ElementValueType LONG = new LongType();
	public static final ElementValueType INTEGER = new IntegerType();
	public static final ElementValueType BOOLEAN = new BooleanType();
	public static final ElementValueType DATE = new DateType();
	public static final ElementValueType BYTE = new ByteType();
	public static final ElementValueType FLOAT = new FloatType();
	public static final ElementValueType DOUBLE = new DoubleType();
	public static final ElementValueType BYTEARRAY = new ByteArrayType();
	public static final ElementValueType OTHERS = new OtherObjectType();

	private static class StringType extends ElementValueType {

	}

	private static class LongType extends ElementValueType {

	}

	private static class IntegerType extends ElementValueType {

	}

	private static class BooleanType extends ElementValueType {

	}

	private static class DateType extends ElementValueType {

	}

	private static class ByteType extends ElementValueType {

	}

	private static class FloatType extends ElementValueType {

	}

	private static class DoubleType extends ElementValueType {

	}

	private static class ByteArrayType extends ElementValueType {

	}

	private static class OtherObjectType extends ElementValueType {

	}
}
