/*
 * Copyright 2016 Crown Copyright
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

package gaffer.function2;

/**
 * An <code>Aggregator</code> {@link gaffer.function2.StatefulFunction} has the same input and output types.
 * @param <T> Aggregated type
 */
public abstract class Aggregator<T> extends StatefulFunction<T, T> {
    public void execute(final T input) {
        aggregate(input);
    }

    /**
     * Aggregate a new input value.
     * @param input Input value
     */
    public abstract void aggregate(T input);

    /**
     * @return New <code>Aggregator</code> of the same type.
     */
    public abstract Aggregator<T> copy();
}
