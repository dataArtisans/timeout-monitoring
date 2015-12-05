/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.timeoutmonitoring;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.json.JSONObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public class TimeoutMonitoringTest {
	@Test
	public void testTimeoutMonitoring() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		List<String> inputData = new ArrayList<>();

		inputData.add("{\"_context_request_id\": \"foo\", \"timestamp\": \"1987-09-30 12:56:12.123456\", \"event_type\": \"bar\", \"publisher_id\": \"api.foobar.novactl.asf\", \"_context_user_name\": \"foobar\"}");
		inputData.add("{\"_context_request_id\": \"foo\", \"timestamp\": \"1987-09-30 12:56:12.523456\", \"event_type\": \"bar\", \"publisher_id\": \"api.foobar.intermediate.asf\", \"_context_user_name\": \"foobar\"}");
		inputData.add("{\"_context_request_id\": \"foo\", \"timestamp\": \"1987-09-30 12:56:13.123456\", \"event_type\": \"compute.instance.create.end\", \"publisher_id\": \"api.foobar.barfoo.asf\", \"_context_user_name\": \"foobar\"}");
		inputData.add("{\"_context_request_id\": \"foobar\", \"timestamp\": \"1987-09-30 12:56:12.123456\", \"event_type\": \"bar\", \"publisher_id\": \"api.foobar.novactl.asf\", \"_context_user_name\": \"foobar\"}");
		inputData.add("{\"_context_request_id\": \"foobar\", \"timestamp\": \"1987-09-30 12:56:13.123456\", \"event_type\": \"compute.instance.create.end\", \"publisher_id\": \"api.foobar.barfoo.asf\", \"_context_user_name\": \"foobar\"}");

		final String[] inputKeys = {"_context_request_id", "timestamp", "event_type", "publisher_id", "_context_user_name"};
		final String key = "_context_request_id";
		final String[] resultFields = {"_context_request_id", "_context_user_name"};

		DataStream<String> input = env.addSource(new SequentialCollectionSource<String>(inputData)).returns(String.class);
		DataStream<JSONObject> jsonObjects = input.map(new MapFunction<String, JSONObject>() {
			@Override
			public JSONObject map(String s) throws Exception {
				return new JSONObject(s);
			}
		});

		@SuppressWarnings("unchecked")
		DataStream<JSONObject> result = JSONSessionMonitoring.createSessionMonitoring(
				jsonObjects, // input data set
				inputKeys, // json elements to keep from the input
				key, // key to group on
				new JSONObjectPredicateRegex("publisher_id", Pattern.compile("api.*novactl.*")), // session start element
				new JSONObjectPredicateEquals<>("event_type", "compute.instance.create.end"), // session end element
				1000, // timeout of 1000 milliseconds
				new LatencyWindowFunction(resultFields) // create the latency from the first and last element of the session
		);

		result.print();

		env.execute();
	}

	public static class SequentialCollectionSource<T> implements SourceFunction<T> {
		private final Collection<T> inputData;
		private boolean running = true;

		public SequentialCollectionSource(Collection<T> inputData) {
			this.inputData = inputData;
		}

		@Override
		public void run(SourceContext<T> sourceContext) throws Exception {
			Iterator<T> iterator = inputData.iterator();
			while(running && iterator.hasNext()) {
				sourceContext.collect(iterator.next());
				Thread.sleep(200);
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}
}
