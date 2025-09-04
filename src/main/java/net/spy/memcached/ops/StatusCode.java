/*
 * arcus-java-client : Arcus Java client
 * Copyright 2020 NAVER Corp.
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
// Copyright (C) 2006-2009 Dustin Sallings
// Copyright (C) 2009-2011 Couchbase, Inc.

package net.spy.memcached.ops;

import java.util.HashMap;
import java.util.Map;

import net.spy.memcached.compat.log.Logger;
import net.spy.memcached.compat.log.LoggerFactory;

public enum StatusCode {

  /*
   Key-Value Statuses
   */
  SUCCESS("SUCCESS"),

  ERR_NOT_FOUND("NOT_FOUND"),
  ERR_EXISTS("EXISTS"),
  ERR_INVAL("INVALID"),
  ERR_NOT_STORED("NOT_STORED"),
  ERR_NOT_SUPPORTED("NOT_SUPPORTED"),

  // error from server (These replace ERR_INTERNAL.),
  ERR_ERROR("ERROR"),
  ERR_SERVER("SERVER_ERROR"),
  ERR_CLIENT("CLIENT_ERROR"),
  ERR_AUTH("AUTH_ERROR"),
  ERR_INTERNAL("INTERNAL_ERROR"),

  // error from client
  CANCELLED("CANCELED"),
  INTERRUPTED("INTERRUPTED"),
  TIMEDOUT("TIMEDOUT"),
  UNDEFINED("UNDEFINED"),
  EXCEPTION("EXCEPTION"),

  // for backward-compatibility with spymemcached.
  // in Arcus ASCII Commands, these are given with SERVER_ERROR / ERROR prefix.
  ERR_2BIG("2BIG"),
  ERR_DELTA_BADVAL("DELTA_BADVAL"),
  ERR_UNKNOWN_COMMAND("UNKNOWN_COMMAND"),
  ERR_NO_MEM("NO_MEM"),

  /*
  Collection & Attribute Statuses
   */
  ERR_TYPE_MISMATCH("TYPE_MISMATCH"),
  ERR_NOT_FOUND_ELEMENT("NOT_FOUND_ELEMENT"),
  ERR_ELEMENT_EXISTS("ELEMENT_EXISTS"),
  ERR_UNREADABLE("UNREADABLE"),
  ERR_OVERFLOWED("OVERFLOWED"),
  ERR_OUT_OF_RANGE("OUT_OF_RANGE"),
  ERR_NOTHING_TO_UPDATE("NOTHING_TO_UPDATE"),
  ERR_ATTR_NOT_FOUND("ATTR_ERROR_NOT_FOUND"),
  ERR_ATTR_BAD_VALUE("ATTR_ERROR_BAD_VALUE"),

  // Piped operation specific
  ERR_FAILED_END("FAILED_END"),

  // Set specific
  EXIST("EXIST"),
  NOT_EXIST("NOT_EXIST"),

  // B+TREE specific
  DUPLICATED("DUPLICATED"),
  TRIMMED("TRIMMED"),
  DUPLICATED_TRIMMED("DUPLICATED_TRIMMED"),
  ERR_BKEY_MISMATCH("BKEY_MISMATCH"),
  ERR_EFLAG_MISMATCH("EFLAG_MISMATCH"),
  ERR_ATTR_MISMATCH("ATTR_MISMATCH");

  private final String message;

  private static final Logger logger =
          LoggerFactory.getLogger(StatusCode.class);

  StatusCode(String message) {
    this.message = message;
  }

  private static final Map<String, StatusCode> ENUM_STRINGS;

  static {
    StatusCode[] values = StatusCode.values();
    ENUM_STRINGS = new HashMap<>(values.length);
    for (StatusCode code : values) {
      ENUM_STRINGS.put(code.message, code);
    }

    ENUM_STRINGS.put("OK", SUCCESS);
    ENUM_STRINGS.put("END", SUCCESS);
    ENUM_STRINGS.put("STORED", SUCCESS);
    ENUM_STRINGS.put("DELETED", SUCCESS);

    ENUM_STRINGS.put("DELETED_DROPPED", SUCCESS);
    ENUM_STRINGS.put("CREATED", SUCCESS);
    ENUM_STRINGS.put("CREATED_STORED", SUCCESS);
    ENUM_STRINGS.put("REPLACED", SUCCESS);
    ENUM_STRINGS.put("UPDATED", SUCCESS);
  }

  public static StatusCode fromAsciiLine(String line) {
    StatusCode statusCode = ENUM_STRINGS.get(line);

    // This case is rarely occurred.
    if (statusCode == null) {
      if (line.startsWith("INVALID")) {
        return ERR_INVAL;
      } else {
        logger.warn("Undefined response message: %s", line);
        return UNDEFINED;
      }
    }

    return statusCode;
  }

}
