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

package gaffer.tuple;

import gaffer.tuple.tuplen.Tuple5;

import java.util.Arrays;
import java.util.Iterator;

/**
 * An <code>ArrayTuple</code> is a simple implementation of the {@link gaffer.tuple.Tuple} interface, backed by an
 * array of {@link java.lang.Object}s, referenced by their index.
 */
public class ArrayTuple extends Tuple5<Object, Object, Object, Object, Object> {
    private final Object[] values;

    /**
     * Create an <code>ArrayTuple</code> backed by the given array.
     * @param values Array backing this <code>ArrayTuple</code>.
     */
    public ArrayTuple(final Object[] values) {
        this.values = values;
    }

    /**
     * Create an <code>ArrayTuple</code> backed by a new array of the given size.
     * @param size Size of array backing this <code>ArrayTuple</code>.
     */
    public ArrayTuple(int size) {
        this.values = new Object[size];
    }

    public Object getValue(final Integer index) {
        return values[index];
    }

    public void putValue(final Integer index, final Object value) {
        values[index] = value;
    }

    public Iterable<Object> values() {
        return Arrays.asList(values);
    }

    public Iterator<Object> iterator() {
        return values().iterator();
    }

    @Override
    public Object get4() {
        return values[4];
    }

    @Override
    public void put4(Object value) {
        values[4] = value;
    }

    @Override
    public Object get3() {
        return values[3];
    }

    @Override
    public void put3(Object value) {
        values[3] = value;
    }

    @Override
    public Object get2() {
        return values[2];
    }

    @Override
    public void put2(Object value) {
        values[2] = value;
    }

    @Override
    public Object get1() {
        return values[1];
    }

    @Override
    public void put1(Object value) {
        values[1] = value;
    }

    @Override
    public Object get0() {
        return values[0];
    }

    @Override
    public void put0(Object value) {
        values[0] = value;
    }
}
