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

import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionTrigger<T> implements Trigger<T, GlobalWindow> {

	private enum WindowState {
		EMPTY,
		START_ELEMENT,
		END_ELEMENT
	}

	private static final Logger LOG = LoggerFactory.getLogger(SessionTrigger.class);

	private final Function<T, Boolean> isSessionStart;
	private final Function<T, Boolean> isSessionEnd;
	private final long timeout;

	public SessionTrigger(
		Function<T, Boolean> isSessionStart,
		Function<T, Boolean> isSessionEnd,
		long timeout) {
		this.isSessionStart = isSessionStart;
		this.isSessionEnd = isSessionEnd;
		this.timeout = timeout;
	}

	@Override
	public TriggerResult onElement(T record, long timestamp, GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {
		OperatorState<WindowState> windowState = triggerContext.getKeyValueState("windowState", WindowState.EMPTY);

		JSONObject object = (JSONObject) record;

		if (isSessionStart.apply(record)) {
			if (windowState.value() == WindowState.EMPTY) {
				windowState.update(WindowState.START_ELEMENT);

				triggerContext.registerEventTimeTimer(timestamp + timeout);

				return TriggerResult.CONTINUE;
			} else if (windowState.value() == WindowState.END_ELEMENT) {
				return TriggerResult.CONTINUE;
			} else {
				LOG.info("Received another start element for the same session.");
				return TriggerResult.CONTINUE;
			}
		} else if (isSessionEnd.apply(record)) {
			if (windowState.value() == WindowState.EMPTY) {
				windowState.update(WindowState.END_ELEMENT);

				triggerContext.registerEventTimeTimer(timestamp);
				return TriggerResult.CONTINUE;
			} else if (windowState.value() == WindowState.START_ELEMENT) {
				return TriggerResult.FIRE_AND_PURGE;
			} else {
				LOG.info("Received another end element for the same session.");
				return TriggerResult.CONTINUE;
			}
		} else {
			return TriggerResult.CONTINUE;
		}
	}

	@Override
	public TriggerResult onProcessingTime(long l, GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {
		throw new UnsupportedOperationException("This trigger does not work with on processing time.");
	}

	@Override
	public TriggerResult onEventTime(long l, GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {
		return TriggerResult.FIRE_AND_PURGE;
	}
}
