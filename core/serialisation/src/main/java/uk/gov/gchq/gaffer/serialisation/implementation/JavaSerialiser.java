/*
 * Copyright 2016-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.serialisation.implementation;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * This class is used to serialise and deserialise objects in java.
 */
public class JavaSerialiser implements ToBytesSerialiser<Object> {
    private static final long serialVersionUID = 2073581763875104361L;
    private static final Class<Serializable> SERIALISABLE = Serializable.class;

    @Override
    public byte[] serialise(final Object object) throws SerialisationException {

        try (final ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
                final ObjectOutputStream out = new ObjectOutputStream(byteOut);) {
            out.writeObject(object);
            return byteOut.toByteArray();
        } catch (final IOException e) {
            throw new SerialisationException("Unable to serialise given object of class: " + object.getClass().getName()
                    + ", does it implement the serializable interface?", e);
        }
    }

    @Override
    public Object deserialise(final byte[] allBytes, final int offset, final int length) throws SerialisationException {
        try (final ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(allBytes, offset, length))) {
            return is.readObject();
        } catch (final ClassNotFoundException | IOException e) {
            throw new SerialisationException("Unable to deserialise object, failed to recreate object", e);
        }
    }

    @Override
    public Object deserialise(final byte[] bytes) throws SerialisationException {
        return deserialise(bytes, 0, bytes.length);
    }

    @Override
    public Object deserialiseEmpty() {
        return null;
    }

    @Override
    public boolean canHandle(final Class clazz) {
        return SERIALISABLE.isAssignableFrom(clazz);
    }

    @Override
    public boolean preservesObjectOrdering() {
        return false;
    }

    @Override
    public boolean isConsistent() {
        return false;
    }

    @Override
    public boolean equals(final Object obj) {
        return this == obj || obj != null && this.getClass() == obj.getClass();
    }

    @Override
    public int hashCode() {
        return JavaSerialiser.class.getName().hashCode();
    }
}
