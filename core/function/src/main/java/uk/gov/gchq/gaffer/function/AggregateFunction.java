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

package uk.gov.gchq.gaffer.function;

/**
 * An <code>AggregateFunction</code> is a {@link uk.gov.gchq.gaffer.function.ConsumerProducerFunction} that reduces a number of
 * input records to a single output of the same record type. The function will update it's internal state in response
 * to new input records, and return the current state on request.
 * <p>
 * For example:<br>
 * <code>AggregateFunction sum = new Sum();</code><br>
 * <code>sum.aggregate({1}); sum.aggregate({2}); sum.aggregate({3});</code><br>
 * <code>Object[] state = sum.state() // state = {6}</code>
 */
public abstract class AggregateFunction extends ConsumerProducerFunction implements Cloneable {
    /**
     * Initialise the internal state of this <code>AggregateFunction</code>.
     * This will normally involve setting an aggregate field back to null.
     */
    public abstract void init();

    /**
     * Execute this <code>AggregateFunction</code> with an input record. Input records should match the types reported
     * by <code>getInputClasses()</code>.
     *
     * @param input Input record.
     */
    public abstract void aggregate(final Object[] input);

    /**
     * @return Record containing the current state of this function.
     */
    public abstract Object[] state();

    /**
     * Create a deep copy of this <code>AggregateFunction</code>, with it's internal state initialised.
     *
     * @return Initialised copy.
     */
    public abstract AggregateFunction statelessClone();
}
