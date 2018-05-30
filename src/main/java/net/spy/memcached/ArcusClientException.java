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
package net.spy.memcached;

public abstract class ArcusClientException extends RuntimeException {

  private static final long serialVersionUID = -4658082759499319120L;

  public ArcusClientException(String message) {
    super(message);
  }

  public ArcusClientException(String message, Throwable e) {
    super(message, e);
  }

  public static class InitializeClientException extends ArcusClientException {

    private static final long serialVersionUID = 2001051171343419920L;

    public InitializeClientException(String message) {
      super(message);
    }

    public InitializeClientException(String message, Throwable e) {
      super(message, e);
    }
  }
}
