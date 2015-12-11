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

import com.dataartisans.timeoutmonitoring.Function;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.JSONObject;

import java.io.IOException;
import java.io.ObjectInputStream;

public class JSONObjectAlertFunction implements Function<Long, JSONObject> {
	private final String timestampKey;
	private final String timestampPattern;
	private transient DateTimeFormatter formatter;

	public JSONObjectAlertFunction(String timestampKey, String timestampPattern) {
		this.timestampKey = timestampKey;
		this.timestampPattern = timestampPattern;
		this.formatter = DateTimeFormat.forPattern(timestampPattern);
	}

	@Override
	public JSONObject apply(Long aLong) {
		JSONObject result = new JSONObject();

		DateTime timestamp = new DateTime(aLong);

		String timestampValue = formatter.print(timestamp);

		result.put(timestampKey, timestampValue);

		return result;
	}

	private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		ois.defaultReadObject();
		formatter = DateTimeFormat.forPattern(timestampPattern);
	}
}
