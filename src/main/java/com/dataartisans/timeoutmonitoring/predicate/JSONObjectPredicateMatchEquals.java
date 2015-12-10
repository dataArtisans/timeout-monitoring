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

package com.dataartisans.timeoutmonitoring.predicate;

public class JSONObjectPredicateMatchEquals<T> extends JSONObjectPredicateMatch<T> {
	private final T value;

	public JSONObjectPredicateMatchEquals(String key, T value) {
		super(key);
		this.value = value;
	}

	@Override
	public boolean match(T element) {
		if (element == null) {
			return value == null;
		} else {
			return element.equals(value);
		}
	}
}
