/*
 * Copyright 2017 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.gov.gchq.gaffer.time;

import java.time.Instant;

/**
 * A time series of type <code>T</code> consists of values of type <code>T</code>
 * associated to time {@link Instant}s.
 *
 * @param <T> The type of the value associated to timestamps.
 */
public interface TimeSeries<T> {

    void put(Instant instant, T t);

    T get(Instant instant);
}
