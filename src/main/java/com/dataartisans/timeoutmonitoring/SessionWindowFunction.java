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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class SessionWindowFunction<IN, OUT, KEY> implements WindowFunction<IN, OUT, KEY, GlobalWindow>, ResultTypeQueryable<OUT> {
	private static final Logger LOG = LoggerFactory.getLogger(SessionWindowFunction.class);

	private final Function<IN, Boolean> isSessionStart;
	private final Function<IN, Boolean> isSessionEnd;
	private final Function<IN, Long> timestampExtractor;
	private final Function<Tuple2<IN, IN>, OUT> windowFunction;
	private final TypeInformation<OUT> outTypeInformation;

	public SessionWindowFunction(
		Function<IN, Boolean> isSessionStart,
		Function<IN, Boolean> isSessionEnd,
		Function<IN, Long> timestampExtractor,
		Function<Tuple2<IN, IN>, OUT> windowFunction,
		Class<OUT> outClass) {
		this.isSessionStart = isSessionStart;
		this.isSessionEnd = isSessionEnd;
		this.timestampExtractor = timestampExtractor;
		this.windowFunction = windowFunction;


		outTypeInformation = TypeExtractor.getForClass(outClass);
	}

	@Override
	public void apply(KEY key, GlobalWindow globalWindow, Iterable<IN> iterable, Collector<OUT> collector) throws Exception {
		Iterator<IN> iterator = iterable.iterator();

		IN firstEvent = iterator.next();
		long firstTimestamp = timestampExtractor.apply(firstEvent);
		IN lastEvent = firstEvent;
		long lastTimestamp = timestampExtractor.apply(lastEvent);

		while (iterator.hasNext()) {
			IN event = iterator.next();
			long eventTimestamp = timestampExtractor.apply(event);

			if (eventTimestamp < firstTimestamp) {
				firstEvent = event;
				firstTimestamp = eventTimestamp;
			}

			if (eventTimestamp > lastTimestamp) {
				lastEvent = event;
				lastTimestamp = eventTimestamp;
			}
		}

		if (!lastEvent.equals(firstEvent)) {
			if (isSessionStart.apply(firstEvent) && isSessionEnd.apply(lastEvent)) {
				collector.collect(windowFunction.apply(Tuple2.of(firstEvent, lastEvent)));
			} else {
				LOG.info("The window does not contain the session end element. This indicates that " +
						"the window has been triggered by the timeout." + firstEvent);
			}
		} else {
			LOG.info("The window does not contain the session end element. This indicates that " +
					"the window has been triggered by the timeout." + firstEvent);
		}
	}

	@Override
	public TypeInformation<OUT> getProducedType() {
		return outTypeInformation;
	}
}
