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

package com.dataartisans.timeoutmonitoring.session;

import com.dataartisans.timeoutmonitoring.Function;
import com.dataartisans.timeoutmonitoring.Function2;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class SessionWindowFunction<IN, OUT, KEY> implements WindowFunction<Iterable<IN>, OUT, KEY, GlobalWindow>, ResultTypeQueryable<OUT> {
	private static final Logger LOG = LoggerFactory.getLogger(SessionWindowFunction.class);

	private final Function<IN, Boolean> isSessionStart;
	private final Function<IN, Boolean> isSessionEnd;
	private final Function<IN, Long> timestampExtractor;
	private final Function2<IN, IN, OUT> windowFunction;
	private final Function<IN, OUT> timeoutFunction;
	private final long timeout;
	private final TypeInformation<OUT> outTypeInformation;

	public SessionWindowFunction(
		Function<IN, Boolean> isSessionStart,
		Function<IN, Boolean> isSessionEnd,
		Function<IN, Long> timestampExtractor,
		Function2<IN, IN, OUT> windowFunction,
		Function<IN, OUT> timeoutFunction,
		long timeout,
		Class<OUT> outClass) {
		this.isSessionStart = isSessionStart;
		this.isSessionEnd = isSessionEnd;
		this.timestampExtractor = timestampExtractor;
		this.windowFunction = windowFunction;
		this.timeoutFunction = timeoutFunction;
		this.timeout = timeout;

		outTypeInformation = TypeExtractor.getForClass(outClass);
	}

	@Override
	public void apply(KEY key, GlobalWindow globalWindow, Iterable<IN> iterable, Collector<OUT> collector) throws Exception {
		Iterator<IN> iterator = iterable.iterator();

		if(!iterator.hasNext()){
			return;
		}

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

		TimestampedCollector<OUT> timestampedCollector;

		if (collector instanceof TimestampedCollector) {
			timestampedCollector = (TimestampedCollector<OUT>)collector;
		} else {
			throw new RuntimeException("The collector must be of type TimestampedCollector " +
				"in order to set the timestamp correctly.");
		}

		if (isSessionStart.apply(firstEvent)) {
			if (!isSessionEnd.apply(lastEvent) || lastTimestamp - firstTimestamp > timeout) {
				timestampedCollector.setAbsoluteTimestamp(firstTimestamp + timeout);
				collector.collect(timeoutFunction.apply(firstEvent));
			} else {
				timestampedCollector.setAbsoluteTimestamp(lastTimestamp);
				collector.collect(windowFunction.apply(firstEvent, lastEvent));
			}
		} else {
			LOG.info("The window does not contain the session start element. This indicates" +
					"missing data.");
		}
	}

	@Override
	public TypeInformation<OUT> getProducedType() {
		return outTypeInformation;
	}

}
