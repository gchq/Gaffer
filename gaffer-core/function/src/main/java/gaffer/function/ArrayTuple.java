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

package gaffer.function;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * An <code>ArrayTuple</code> is a simple implementation of the {@link gaffer.function.Tuple} interface, backed by an
 * array of {@link java.lang.Object}s, referenced by their index. This implementation is read-only - calls to
 * <code>put(Integer, Object)</code> will result in an {@link java.lang.UnsupportedOperationException} being thrown.
 */
public class ArrayTuple implements Tuple<Integer> {
    private final Object[] tuple;

    /**
     * Create an <code>ArrayTuple</code> backed by the given array.
     *
     * @param tuple Array backing this <code>ArrayTuple</code>.
     */
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "This class is designed to simply wrap an object array.")
    public ArrayTuple(final Object[] tuple) {
        this.tuple = tuple;
    }

    @Override
    public Object get(final Integer index) {
        return tuple[index];
    }

    @Override
    public void put(final Integer reference, final Object value) {
        throw new UnsupportedOperationException("'puts are not supported with this Tuple");
    }
}
