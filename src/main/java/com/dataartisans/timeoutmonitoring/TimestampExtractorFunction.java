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

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.JSONObject;

import java.io.IOException;
import java.io.ObjectInputStream;

public class TimestampExtractorFunction implements Function<JSONObject, Long> {

	private final String timestampField;
	private final String pattern;
	private transient DateTimeFormatter formatter;

	public TimestampExtractorFunction(String timestampField, String pattern) {
		this.timestampField = timestampField;
		this.pattern = pattern;
		formatter = DateTimeFormat.forPattern(pattern);
	}

	@Override
	public Long apply(JSONObject jsonObject) {
		String timestamp = jsonObject.optString(timestampField);

		if (timestamp == null) {
			return Long.MIN_VALUE;
		} else {
			DateTime dateTime = formatter.parseDateTime(timestamp);
			return dateTime.getMillis();
		}
	}

	private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		ois.defaultReadObject();
		formatter = DateTimeFormat.forPattern(pattern);
	}
}
