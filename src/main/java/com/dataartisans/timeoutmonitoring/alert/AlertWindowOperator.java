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
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.shaded.com.google.common.collect.EvictingQueue;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.TreeSet;

public class AlertWindowOperator<IN, OUT>
	extends AbstractStreamOperator<OUT>
	implements OneInputStreamOperator<IN, OUT> {

	private final int numberEvents;
	private final long windowInterval;
	private final Function<Long, OUT> alertFunction;
	private final TreeSet<AlertEvent> alertEvents;

	public AlertWindowOperator(
			int numberEvents,
			long windowInterval,
			Function<Long, OUT> alertFunction) {
		this.numberEvents = numberEvents;
		this.windowInterval = windowInterval;
		this.alertFunction = alertFunction;
		this.alertEvents = new TreeSet<>(new AlertEvent.AlertEventComparator());
	}

	@Override
	public void processElement(StreamRecord<IN> streamRecord) throws Exception {
		AlertEvent alertEvent = new AlertEvent<>(streamRecord.getTimestamp(), streamRecord.getValue());
		alertEvents.add(alertEvent);
		generateAlerts(alertEvent);
	}

	public void generateAlerts(AlertEvent alertEvent) {
		if (alertEvents.size() >= numberEvents) {
			EvictingQueue<AlertEvent> ringBuffer = EvictingQueue.create(numberEvents - 1);
			Iterator<AlertEvent> alertIterator = alertEvents.iterator();
			StreamRecord<OUT> streamRecord = new StreamRecord<>(null);

			for (int i = 0; i < numberEvents - 1; i++) {
				ringBuffer.add(alertIterator.next());
			}

			boolean alertEventProcessed = false;

			while (alertIterator.hasNext() && !alertEventProcessed) {
				AlertEvent oldestElement = alertIterator.next();
				AlertEvent youngestElement = ringBuffer.poll();

				if (!youngestElement.isProcessed()) {
					if (youngestElement.getTimestamp() - oldestElement.getTimestamp() <= windowInterval) {
						youngestElement.markProcessed();

						streamRecord.replace(alertFunction.apply(youngestElement.getTimestamp()), youngestElement.getTimestamp());

						output.collect(streamRecord);
					}

					if (youngestElement.equals(alertEvent)) {
						alertEventProcessed = true;
					}
				}

				ringBuffer.add(oldestElement);
			}
		}
	}

	@Override
	public void processWatermark(Watermark watermark) throws Exception {
		long discardingTimestamp = watermark.getTimestamp() - windowInterval;

		while (!alertEvents.isEmpty() && alertEvents.last().getTimestamp() < discardingTimestamp) {
			alertEvents.pollLast();
		}
	}

	@Override
	public StreamTaskState snapshotOperatorState(long checkpointId, long timestamp) throws Exception {
		StreamTaskState taskState = super.snapshotOperatorState(checkpointId, timestamp);

		AbstractStateBackend.CheckpointStateOutputView out = getStateBackend().createCheckpointStateOutputView(checkpointId, timestamp);

		ObjectOutputStream oos = new ObjectOutputStream(out);

		oos.writeInt(alertEvents.size());

		for (AlertEvent event: alertEvents) {
			oos.writeObject(event);
		}

		oos.close();

		taskState.setOperatorState(out.closeAndGetHandle());

		return taskState;
	}

	@Override
	public void restoreState(StreamTaskState taskState, long recoveryTimestamp) throws Exception {
		@SuppressWarnings("unchecked")
		StateHandle<DataInputView> stateHandle = (StateHandle<DataInputView>)taskState.getOperatorState();

		DataInputView input = stateHandle.getState(getUserCodeClassloader());

		ObjectInputStream ois = new ObjectInputStream(new DataInputViewStream(input));

		int numAlertEvents = ois.readInt();

		for (int i = 0; i < numAlertEvents; i++) {
			AlertEvent alertEvent = (AlertEvent)ois.readObject();
			alertEvents.add(alertEvent);
		}

		super.restoreState(taskState, recoveryTimestamp);
	}

}
