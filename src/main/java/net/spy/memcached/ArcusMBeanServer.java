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

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

public final class ArcusMBeanServer {

	private final MBeanServer mbserver;

	private static class SingletonHolder {
		private final static ArcusMBeanServer INSTANCE = new ArcusMBeanServer();
	}

	private ArcusMBeanServer() {
		mbserver = ManagementFactory.getPlatformMBeanServer();
	}

	public static ArcusMBeanServer getInstance() {
		return SingletonHolder.INSTANCE;
	}

	public boolean isRegistered(String name) {
		try {
			return mbserver != null
					&& mbserver.isRegistered(new ObjectName(name));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void registMBean(Object o, String name) {
		if (isRegistered(name)) {
			return;
		}

		if (mbserver != null) {
			try {
				mbserver.registerMBean(o, new ObjectName(name));
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}
}
