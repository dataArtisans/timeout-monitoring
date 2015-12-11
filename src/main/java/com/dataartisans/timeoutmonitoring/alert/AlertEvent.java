/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.timeoutmonitoring.alert;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;

public class AlertEvent implements Serializable {
	private static long serialVersionUID = 1L;

	private long timestamp;
	private boolean processed;

	public AlertEvent(long timestamp) {
		this.timestamp = timestamp;
		this.processed = false;
	}

	@Override
	public int hashCode() {
		return Objects.hash(timestamp);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof AlertEvent) {
			AlertEvent other = (AlertEvent) obj;

			return other.canEqual(this)	&& timestamp == other.timestamp;
		} else {
			return false;
		}
	}

	public boolean canEqual(AlertEvent event) {
		return event instanceof AlertEvent;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public void markProcessed() {
		this.processed = true;
	}

	public boolean isProcessed() {
		return processed;
	}

	public static class AlertEventComparator implements Comparator<AlertEvent>, Serializable {

		@Override
		public int compare(AlertEvent o1, AlertEvent o2) {
			if (o1.getTimestamp() > o2.getTimestamp()) {
				return -1;
			} else if (o1.getTimestamp() < o2.getTimestamp()) {
				return 1;
			} else {
				return 0;
			}
		}
	}
}
