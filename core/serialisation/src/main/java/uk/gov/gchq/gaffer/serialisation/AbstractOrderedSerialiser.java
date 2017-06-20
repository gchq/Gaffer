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

package uk.gov.gchq.gaffer.serialisation;

import com.google.common.base.Preconditions;
import java.util.Objects;


/**
 * A class that extends this abstract class is responsible for serialising an
 * object of class T to a byte array, and for deserialising it back again.
 * It must also be able to deal with serialising null values.
 * It must also preserve object ordering.
 */
public abstract class AbstractOrderedSerialiser<T> implements Serialiser<T, byte[]> {

    /**
     * Decodes a byte array without checking if the offset and len exceed the bounds of the actual array.
     *
     * @param b      Bytes to be deserialised.
     * @param offset Offset.
     * @param len    Bytes length.
     * @return T     Deserialised object.
     */
    protected abstract T deserialiseUnchecked(final byte[] b, final int offset, final int len);

    /**
     * Serialise some object and returns the raw bytes of the serialised form.
     *
     * @param obj Object to be serialised.
     * @return byte[] Serialised bytes.
     */
    @Override
    public abstract byte[] serialise(final T obj);

    /**
     * Handle an empty byte array and reconstruct an appropriate representation in T form.
     *
     * @return T returns null.
     */
    @Override
    public T deserialiseEmpty() {
        return null;
    }

    /**
     * Handle an incoming null value and generate an appropriate byte array representation.
     *
     * @return byte[] Serialised bytes.
     */
    @Override
    public byte[] serialiseNull() {
        return new byte[0];
    }

    /**
     * Returns a boolean indicating whether the ordering of object T is preserved.
     * Anything that extends this class must preserve object ordering.
     *
     * @return boolean returns true. All classes that extend must preserve object ordering.
     */
    @Override
    public boolean preservesObjectOrdering() {
        return true;
    }

    /**
     * Checks the byte array is not null, then calls {@link #deserialise(byte[], int, int)}.
     *
     * @param b Bytes to be deserialised.
     * @return T Deserialised object.
     * @throws java.lang.NullPointerException if {@code b} is null.
     */
    @Override
    public T deserialise(final byte[] b) {
        Objects.requireNonNull(b, "cannot decode null byte array");
        return this.deserialiseUnchecked(b, 0, b.length);
    }

    /**
     * Checks the byte array is not null, and parameters do not exceed the bounds of the byte array, then calls {@link #deserialiseUnchecked(byte[], int, int)}.
     *
     * @param b      Bytes to be deserialised.
     * @param len    Bytes length.
     * @param offset Offset.
     * @return T Deserialised object.
     * @throws java.lang.NullPointerException     if {@code b} is null.
     * @throws java.lang.IllegalArgumentException if {@code offset + len} exceeds the length of {@code b}.
     */
    public T deserialise(final byte[] b, final int offset, final int len) {
        Objects.requireNonNull(b, "cannot decode null byte array");
        Preconditions.checkArgument(offset >= 0, "offset %s cannot be negative", new Object[]{Integer.valueOf(offset)});
        Preconditions.checkArgument(len >= 0, "length %s cannot be negative", new Object[]{Integer.valueOf(len)});
        Preconditions.checkArgument(offset + len <= b.length, "offset + length %s exceeds byte array length %s", new Object[]{Integer.valueOf(offset + len), Integer.valueOf(b.length)});
        return this.deserialiseUnchecked(b, offset, len);
    }
}
