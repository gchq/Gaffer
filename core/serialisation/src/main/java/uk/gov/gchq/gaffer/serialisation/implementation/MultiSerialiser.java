/*
 * Copyright 2018 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.gov.gchq.gaffer.serialisation.implementation;

import uk.gov.gchq.gaffer.core.exception.GafferCheckedException;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.CompactRawIntegerSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.CompactRawLongSerialiser;
import uk.gov.gchq.gaffer.serialisation.util.MultiSerialiserStorage;

import java.io.ByteArrayOutputStream;

public class MultiSerialiser implements ToBytesSerialiser<Object> {
    private static final long serialVersionUID = 8206706506883696003L;
    private MultiSerialiserStorage supportedSerialisers = new MultiSerialiserStorage();

    public MultiSerialiser() {
        try {
            supportedSerialisers.put(((byte) 0), StringSerialiser.class, String.class);
            supportedSerialisers.put(((byte) 1), CompactRawLongSerialiser.class, Long.class);
            supportedSerialisers.put(((byte) 2), CompactRawIntegerSerialiser.class, Integer.class);
        } catch (GafferCheckedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public byte[] serialise(final Object object) throws SerialisationException {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            ToBytesSerialiser serialiserFromValue = supportedSerialisers.getSerialiserFromValue(object);
            byte[] serialise = serialiserFromValue.serialise(object);
            byte key = supportedSerialisers.getKeyFromSerialiser(serialiserFromValue);
            byte[] bytes;

            stream.write(key);
            stream.write(serialise);
            bytes = stream.toByteArray();
            return bytes;
        } catch (SerialisationException e) {
            //re-throws SerialisationException
            throw e;
        } catch (Exception e) {
            //wraps other exceptions.
            throw new SerialisationException(e.getMessage(), e);
        }
    }

    @Override
    public Object deserialise(final byte[] bytes) throws SerialisationException {
        try {
            byte aByte = bytes[0];
            ToBytesSerialiser serialiser = supportedSerialisers.getSerialiserFromKey(aByte);
            return serialiser.deserialise(bytes, 1, bytes.length - 1);
        } catch (SerialisationException e) {
            throw e;
        } catch (Exception e) {
            throw new SerialisationException(e.getMessage(), e);
        }
    }

    @Override
    public Object deserialiseEmpty() throws SerialisationException {
        return null;
    }

    @Override
    public boolean preservesObjectOrdering() {
        return supportedSerialisers.isPreservesObjectOrdering();
    }


    @Override
    public boolean isConsistent() {
        return supportedSerialisers.isConsistent();
    }

    @Override
    public boolean canHandle(final Class clazz) {
        try {
            return supportedSerialisers.canHandle(clazz);
        } catch (GafferCheckedException e) {
            throw new RuntimeException("MultiSerialiser unable to recover from canHandle()", e);
        }
    }


}
