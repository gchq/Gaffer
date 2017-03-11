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
 * @param <FI> The adapted input type.
 */
public class TupleInputAdapter<R, FI> implements Function<Tuple<R>, FI> {
    private R[] selection;

    /**
     * Create a new <code>TupleMask</code>.
     */
    public TupleInputAdapter() {
    }

    /**
     * Create a new <code>TupleMask</code> with the given field references.
     *
     * @param selection Field references.
     */
    public TupleInputAdapter(final R... selection) {
        this.selection = selection;
    }

    @Override
    public FI apply(final Tuple<R> input) {
        if (null == selection) {
            throw new IllegalArgumentException("Selection is required");
        }

        if (null != input) {
            if (1 == selection.length) {
                return (FI) input.get(selection[0]);
            }
        }

        return (FI) new ReferenceArrayTuple<>(input, selection);
    }

    /**
     * @return Field references.
     */
    public R[] getSelection() {
        return selection;
    }

    /**
     * Set this <code>TupleMask</code> to refer to a tuple of field references.
     *
     * @param selection Field references.
     */
    public void setSelection(final R... selection) {
        this.selection = selection;
    }
}
