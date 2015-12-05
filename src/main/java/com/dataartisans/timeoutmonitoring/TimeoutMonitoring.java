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
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.json.JSONObject;

import java.util.regex.Pattern;

public class TimeoutMonitoring {

	public static void main(String[] args) throws Exception {
		ParameterTool params = ParameterTool.fromArgs(args);

		String filePath = params.get("filePath");

		if (filePath == null) {
			System.out.println("Job requires the --filePath option to be specified.");
		} else {

			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

			final String[] inputKeys = {"_context_request_id", "payload:instance_type_id", "timestamp", "event_type", "publisher_id", "_context_user_name", "_context_project_name", "_context_tenant", "_context_project_id"};
			final String key = "_context_request_id";
			final String[] resultFields = {"_context_request_id", "payload:instance_type_id", "_context_user_name", "_context_project_name", "_context_tenant", "_context_project_id"};

			DataStream<String> input = env.readTextFile(filePath);
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
				20000, // timeout of 20000 milliseconds
				new LatencyWindowFunction(resultFields) // create the latency from the first and last element of the session
			);

			result.print();

			env.execute("Execute timeout monitoring");
		}
	}
}
