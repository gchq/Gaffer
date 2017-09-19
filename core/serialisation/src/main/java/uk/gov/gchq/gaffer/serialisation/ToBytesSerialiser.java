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

import uk.gov.gchq.gaffer.exception.SerialisationException;

/**
 * A class that implements this interface is responsible for serialising an
 * object of class T to a byte array, and for deserialising it back again.
 * It must also be able to deal with serialising null values.
 */
public interface ToBytesSerialiser<T> extends Serialiser<T, byte[]> {

    /**
     * Handle an incoming null value and generate an appropriate {@code byte[]} representation.
     */
    byte[] EMPTY_BYTES = new byte[0];

    /**
     * Handle an incoming null value and generate an appropriate byte array representation.
     *
     * @return byte[] the serialised bytes
     */
    @Override
    default byte[] serialiseNull() {
        return EMPTY_BYTES;
    }

    /**
     * Serialise some object and returns the raw bytes of the serialised form.
     *
     * @param object the object to be serialised
     * @return byte[] the serialised bytes
     * @throws SerialisationException if the object fails to serialise
     */
    @Override
    byte[] serialise(final T object) throws SerialisationException;

    /**
     * @param allBytes The bytes to be decoded into characters
     * @param offset   The index of the first byte to decode
     * @param length   The number of bytes to decode
     * @return T the deserialised object
     * @throws SerialisationException issues during deserialisation
     */
    default T deserialise(final byte[] allBytes, final int offset, final int length) throws SerialisationException {
        final byte[] selection = new byte[length];
        try {
            System.arraycopy(allBytes, offset, selection, 0, length);
        } catch (final NullPointerException e) {
            throw new SerialisationException(String.format("Deserialising with giving range caused ArrayIndexOutOfBoundsException. byte[].size:%d startPos:%d length:%d", allBytes.length, 0, length), e);
        }
        return deserialise(selection);
    }

    /**
     * Deserialise an array of bytes into the original object.
     *
     * @param bytes the bytes to deserialise
     * @return T the deserialised object
     * @throws SerialisationException if the object fails to deserialise
     *
     * Note that this implementation is less efficient than using deserialise
     * with an offset and a length, but may still be used if necessary.
     *
     * @see #deserialise(byte[], int, int)
     */
    @Override
    T deserialise(final byte[] bytes) throws SerialisationException;

    /**
     * Handle an empty byte array and reconstruct an appropriate representation in T form.
     *
     * @return T the deserialised object
     * @throws SerialisationException if the object fails to deserialise
     */
    @Override
    T deserialiseEmpty() throws SerialisationException;

    /**
     * Indicates whether the serialisation process preserves the ordering of the T,
     * i.e. if x and y are objects of class T, and x is less than y, then this method should
     * return true if the serialised form of x is guaranteed to be less than the serialised form
     * of y (using the standard ordering of byte arrays).
     * If T is not Comparable then this test makes no sense and false should be returned.
     *
     * @return true if the serialisation will preserve the order of the T, otherwise false.
     */
    @Override
    boolean preservesObjectOrdering();
}
