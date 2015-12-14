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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;

public class Alert {

	/**
	 * Creates an alert stream which issues alerts whenever the input contained enough elements
	 * in a given interval.
	 *
	 * @param input Input stream
	 * @param alertName Name of the alert operator
	 * @param numberEventsToTrigger Number of elements to trigger an alert
	 * @param intervalLength The interval in which the events have to occur (milliseconds)
	 * @param alertFunction Function which is called to generate the alert event
	 * @param outTypeInformation Output type information of the alert
	 * @param <IN> Type of the input stream
	 * @param <OUT> Type of the generated alerts
	 * @return A data stream containing the alert events
	 */
	public static <IN, OUT> DataStream<OUT> createAlert(
		DataStream<IN> input,
		String alertName,
		int numberEventsToTrigger,
		long intervalLength,
		Function<Long, OUT> alertFunction,
		TypeInformation<OUT> outTypeInformation) {

		return input.transform(
			alertName,
			outTypeInformation,
			new AlertWindowOperator<IN, OUT>(
				numberEventsToTrigger,
				intervalLength,
				alertFunction
			)
		).setParallelism(1);

	}
}
