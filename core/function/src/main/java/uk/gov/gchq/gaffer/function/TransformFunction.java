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
 * A <code>TransformFunction</code> is a {@link uk.gov.gchq.gaffer.function.Function} that produces a new output record based on
 * the data in an input record.
 * <p>
 * For example:<br>
 * <code>TransformFunction concat = new Concat("|");</code><br>
 * <code>Object[] result = concat.transform({"a", "b"}); // result = {"a|b"}</code>
 */
public abstract class TransformFunction extends ConsumerProducerFunction {
    /**
     * Execute this <code>TransformFunction</code> with an input record to produce a new output record.
     *
     * @param input Input record.
     * @return Output record.
     */
    public abstract Object[] transform(final Object[] input);

    @Override
    public abstract TransformFunction statelessClone();
}
