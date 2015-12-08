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

package com.dataartisans.timeoutmonitoring;

import org.apache.flink.streaming.api.functions.TimestampExtractor;
import org.json.JSONObject;

import java.io.Serializable;

public class JSONObjectTimestampExtractor implements TimestampExtractor<JSONObject>, Serializable {

	private final Function<JSONObject, Long> timestampExtractor;
	private final long delay;

	private long currentWatermark = Long.MIN_VALUE;

	public JSONObjectTimestampExtractor(Function<JSONObject, Long> timestampExtractor, long delay) {
		this.timestampExtractor = timestampExtractor;
		this.delay = delay;
	}

	@Override
	public long extractTimestamp(JSONObject jsonObject, long l) {
		return timestampExtractor.apply(jsonObject);
	}

	@Override
	public long extractWatermark(JSONObject jsonObject, long l) {
		currentWatermark = extractTimestamp(jsonObject, l) - delay;

		return currentWatermark;
	}

	@Override
	public long getCurrentWatermark() {
		return currentWatermark;
	}
}
