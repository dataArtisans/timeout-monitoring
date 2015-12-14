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

import com.dataartisans.timeoutmonitoring.alert.AlertWindowOperator;
import com.dataartisans.timeoutmonitoring.alert.JSONObjectAlertFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.json.JSONObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class AlertGenerationTest {

	@Test
	public void testAlertGeneration() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		List<String> inputData = new ArrayList<>();

		inputData.add("{\"timestamp\": \"1987-09-30 12:56:12.123456\", \"event_type\": \"error\" }");
		inputData.add("{\"timestamp\": \"1987-09-30 12:56:13.123456\", \"event_type\": \"error\" }");
		inputData.add("{\"timestamp\": \"1987-09-30 12:56:14.123456\", \"event_type\": \"error\" }");
		inputData.add("{\"timestamp\": \"1987-09-30 12:56:15.123456\", \"event_type\": \"error\" }");
		inputData.add("{\"timestamp\": \"1987-09-30 12:56:16.123456\", \"event_type\": \"error\" }");

		DataStream<String> stringInput = env.fromCollection(inputData);

		String timestampPattern = "yyyy-MM-dd HH:mm:ss.SSSSSS";

		TimestampExtractorFunction timestampExtractor = new TimestampExtractorFunction("timestamp", timestampPattern);

		DataStream<JSONObject> input = stringInput.map(new MapFunction<String, JSONObject>() {
			@Override
			public JSONObject map(String s) throws Exception {
				return new JSONObject(s);
			}
		}).assignTimestamps(new JSONObjectTimestampExtractor(timestampExtractor, 0));

		TypeInformation<JSONObject> outTypeInformation = TypeExtractor.getForClass(JSONObject.class);

		Function<Long, JSONObject> alertFunction = new JSONObjectAlertFunction(
			"alert",
			"testAlert",
			"timestamp",
			timestampPattern);

		@SuppressWarnings("unchecked")
		DataStream<JSONObject> result = input.transform(
			"AlertWindow",
			outTypeInformation,
			new AlertWindowOperator(5, 5000, alertFunction)
		).setParallelism(1);

		result.print();

		env.execute();
	}
}
