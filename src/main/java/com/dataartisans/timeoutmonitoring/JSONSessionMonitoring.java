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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.json.JSONObject;

public class JSONSessionMonitoring {

	/**
	 * Creates a session monitoring job with Flink.
	 *
	 * @param input DataStream of JSONObjects to be used as the input
	 * @param inputKeys Array of keys which are kept from the original JSONObject input
	 * @param key JSONObject key field to group on
	 * @param isSessionStart JSONObjectPredicate function which detects the session starting elements
	 * @param isSessionEnd JSONObjectPredicate function which detects the session ending elements
	 * @param timeout Timeout after which the session will be discarded
	 * @param windowFunction Function which is called with the session start and end element
	 * @return DataStream of JSONObjects which are produced by the windowFunction
	 */
	public static DataStream createSessionMonitoring(
		DataStream<JSONObject> input,
		final String[] inputKeys,
		final String key,
		final JSONObjectPredicate<?> isSessionStart,
		final JSONObjectPredicate<?> isSessionEnd,
		final long timeout,
		Function<Tuple2<JSONObject, JSONObject>, JSONObject> windowFunction) {

		DataStream<JSONObject> filteredInput = input.map(new MapFunction<JSONObject, JSONObject>() {
			@Override
			public JSONObject map(JSONObject jsonObject) throws Exception {
				return JSONObjectExtractor.createJSONObject(jsonObject, inputKeys);
			}
		});

		return filteredInput
			.keyBy(new KeySelector<JSONObject, Object>() {
				@Override
				public Object getKey(JSONObject jsonObject) throws Exception {
					return jsonObject.get(key);
				}
			})
			.window(new SessionWindowAssigner<>(
				isSessionStart,
				isSessionEnd,
				false))
			.trigger(new SessionTrigger<>(
				isSessionStart,
				isSessionEnd,
				timeout))
			.apply(new SessionWindowFunction<>(
				isSessionStart,
				isSessionEnd,
				windowFunction,
				JSONObject.class
			));
	}
}
