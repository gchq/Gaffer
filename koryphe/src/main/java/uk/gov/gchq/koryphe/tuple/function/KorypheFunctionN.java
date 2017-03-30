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
package uk.gov.gchq.koryphe.tuple.function;

import uk.gov.gchq.koryphe.function.KorypheFunction;
import uk.gov.gchq.koryphe.tuple.Tuple;

public abstract class KorypheFunctionN<TUPLE extends Tuple<Integer>, R> extends KorypheFunction<TUPLE, R> {
    @Override
    public R apply(final TUPLE tuple) {
        if (null == tuple) {
            throw new IllegalArgumentException("Input tuple is required");
        }

        try {
            return delegateApply(tuple);
        } catch (final ClassCastException e) {
            throw new IllegalArgumentException("Input tuple values do not match the required function input types", e);
        }
    }

    protected abstract R delegateApply(final TUPLE tuple);
}
