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

package uk.gov.gchq.koryphe.tuple;


import java.util.Arrays;
import java.util.function.BiFunction;

/**
 * @param <R>  The type of reference used by tuples.
 * @param <FO> The adapted output type.
 */
public class TupleOutputAdapter<R, FO> implements BiFunction<FO, Tuple<R>, Tuple<R>> {
    private R[] projection;

    /**
     * Create a new <code>TupleMask</code>.
     */
    public TupleOutputAdapter() {
        projection = (R[]) new Object[0];
    }

    /**
     * Create a new <code>TupleMask</code> with the given field references.
     *
     * @param projection Field references.
     */
    @SafeVarargs
    public TupleOutputAdapter(final R... projection) {
        setProjection(projection);
    }

    @Override
    public Tuple<R> apply(final FO output, final Tuple<R> state) {
        if (null == projection) {
            throw new IllegalArgumentException("Projection is required");
        }

        if (null != state) {
            if (1 == projection.length) {
                state.put(projection[0], output);
            } else {
                int i = 0;
                for (final Object obj : (Iterable) output) {
                    state.put(projection[i++], obj);
                }
            }
        }

        return state;
    }

    /**
     * Set this <code>TupleMask</code> to refer to a tuple of field references.
     *
     * @param projection Field references.
     */
    @SafeVarargs
    public final void setProjection(final R... projection) {
        if (null == projection) {
            this.projection = (R[]) new Object[0];
        } else {
            this.projection = projection;
        }
    }

    /**
     * @return Field references.
     */
    public R[] getProjection() {
        return Arrays.copyOf(projection, projection.length);
    }
}
