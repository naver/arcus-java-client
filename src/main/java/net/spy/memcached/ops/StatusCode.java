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

import net.spy.memcached.compat.log.Logger;
import net.spy.memcached.compat.log.LoggerFactory;

public enum StatusCode {

  SUCCESS,
  ERR_NOT_FOUND,
  ERR_EXISTS,
  ERR_2BIG,
  ERR_INVAL,
  ERR_NOT_STORED,
  ERR_DELTA_BADVAL,
  ERR_TYPE_MISMATCH,
  ERR_UNKNOWN_COMMAND,
  ERR_NO_MEM,
  ERR_NOT_SUPPORTED,
  ERR_ERROR,
  ERR_SERVER,
  ERR_CLIENT,
  CANCELLED,
  INTERRUPTED,
  TIMEDOUT,
  UNDEFINED;

  private static final Logger logger =
          LoggerFactory.getLogger(StatusCode.class);

  public static StatusCode fromAsciiLine(String line) {
    if (line.equals("OK") || line.equals("END") ||
        line.equals("STORED") || line.equals("DELETED")) {
      return SUCCESS;
    } else if (line.equals("NOT_STORED")) {
      return ERR_NOT_STORED;
    } else if (line.equals("EXISTS")) {
      return ERR_EXISTS;
    } else if (line.equals("NOT_FOUND")) {
      return ERR_NOT_FOUND;
    } else if (line.equals("TYPE_MISMATCH")) {
      return ERR_TYPE_MISMATCH;
    } else if (line.equals("ERROR")) {
      return ERR_ERROR;
    } else if (line.equals("SERVER_ERROR")) {
      return ERR_SERVER;
    } else if (line.equals("CLIENT_ERROR")) {
      return ERR_CLIENT;
    } else {
      logger.warn("Undefined response message: %s", line);
      return UNDEFINED;
    }
  }

}
