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

import org.apache.flink.api.java.tuple.Tuple2;
import org.json.JSONObject;

import java.util.List;

public class JSONObjectExtractor {
	private static String NAME_SEPARATOR = ":";

	public static JSONObject createJSONObject(JSONObject object, String[] fields) {
		JSONObject result = new JSONObject();

		for (String field: fields) {
			Object value = extractValue(object, field);

			insertValue(result, field, value);
		}

		return result;
	}

	public static JSONObject createJSONObject(JSONObject jsonObject, Tuple2<String, String>[] fieldMappings) {
		JSONObject result = new JSONObject();

		for (Tuple2<String, String> fieldMapping: fieldMappings) {
			Object value = extractValue(jsonObject, fieldMapping.f0);

			insertValue(result, fieldMapping.f1, value);
		}

		return result;
	}

	public static Object extractValue(JSONObject jsonObject, String key) {
		String[] keys = key.split(NAME_SEPARATOR);

		for (int i = 0; i < keys.length - 1; i++) {
			jsonObject = jsonObject.getJSONObject(keys[i]);
		}

		return jsonObject.opt(keys[keys.length - 1]);
	}

	public static void insertValue(JSONObject jsonObject, String key, Object value) {
		String[] keys = key.split(NAME_SEPARATOR);

		for (int i = 0; i < keys.length - 1; i++) {
			JSONObject nestedObject = new JSONObject();
			jsonObject.put(keys[i], nestedObject);

			jsonObject = nestedObject;
		}

		jsonObject.putOpt(keys[keys.length - 1], value);
	}
}
