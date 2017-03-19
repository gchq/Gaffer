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
import java.io.Serializable;

/**
 * A class that implements this interface is responsible for serialising an
 * object of class T to a byte array, and for deserialising it back again.
 *
 * It must also be able to deal with serialising null values.
 */
public interface Serialisation<T> extends Serializable {

    byte[] EMPTY_BYTES = new byte[0];

    /**
     * Handle an incoming null value and generate an appropriate byte array representation.
     *
     * @return byte[] the serialised bytes
     */
    default byte[] serialiseNull() {
        return EMPTY_BYTES;
    }

    /**
     * Check whether the serialiser can serialise a particular class.
     *
     * @param clazz the object class to serialise
     * @return boolean true if it can be handled
     */
    boolean canHandle(final Class clazz);

    /**
     * Serialise some object and returns the raw bytes of the serialised form.
     *
     * @param object the object to be serialised
     * @return byte[] the serialised bytes
     * @throws SerialisationException if the object fails to serialise
     */
    byte[] serialise(final T object) throws SerialisationException;

    /**
     * Deserialise an array of bytes into the original object.
     *
     * @param bytes the bytes to deserialise
     * @return T the deserialised object
     * @throws SerialisationException if the object fails to deserialise
     */
    T deserialise(final byte[] bytes) throws SerialisationException;

    /**
     * Handle an empty byte array and reconstruct an appropriate representation in Object form.
     *
     * @return T the deserialised object
     * @throws SerialisationException if the object fails to deserialise
     */
    T deserialiseEmptyBytes() throws SerialisationException;

    /**
     * Indicates whether the serialisation process preserves the ordering of the T,
     * i.e. if x and y are objects of class T, and x is less than y, then this method should
     * return true if the serialised form of x is guaranteed to be less than the serialised form
     * of y (using the standard ordering of byte arrays).
     *
     * If T is not Comparable then this test makes no sense and false should be returned.
     *
     * @return true if the serialisation will preserve the order of the T, otherwise false.
     */
    boolean preservesObjectOrdering();

}
