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


import java.util.function.Function;

/**
 * @param <R>  The type of reference used by tuples.
 * @param <FO> The adapted output type.
 */
public class TupleReverseOutputAdapter<R, FO> implements Function<Tuple<R>, FO> {
    private R[] projection;

    /**
     * Create a new <code>TupleMask</code>.
     */
    public TupleReverseOutputAdapter() {
    }

    /**
     * Create a new <code>TupleMask</code> with the given field references.
     *
     * @param projection Field references.
     */
    @SafeVarargs
    public TupleReverseOutputAdapter(final R... projection) {
        this.projection = projection;
    }

    @Override
    public FO apply(final Tuple<R> output) {
        if (null == projection) {
            throw new IllegalArgumentException("Projection is required");
        }


        if (1 == projection.length) {
            return (FO) output.get(projection[0]);
        }

        return (FO) new ReferenceArrayTuple<>(output, projection);
    }

    /**
     * Set this <code>TupleMask</code> to refer to a tuple of field references.
     *
     * @param projection Field references.
     */
    @SafeVarargs
    public final void setProjection(final R... projection) {
        this.projection = projection;
    }

    /**
     * @return Field references.
     */
    public R[] getProjection() {
        return projection;
    }
}
