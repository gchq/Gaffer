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
import uk.gov.gchq.gaffer.serialisation.util.MultiSerialiserStorage;
import uk.gov.gchq.gaffer.serialisation.util.MultiSerialiserStorage.Content;

import java.io.ByteArrayOutputStream;
import java.util.List;

/**
 * This class is used to serialise and deserialise multiple object types.
 * <p>
 * The serialiser used is stored at the first byte of the serial byte[].
 * This mean encoding can only go up to the size of a byte (256).
 * For multiple serialisers that operate on the same value type. The order of adding matters, last in first out, regardless of the key value.
 */
public class MultiSerialiser implements ToBytesSerialiser<Object> {
    private static final long serialVersionUID = 8206706506883696003L;
    private MultiSerialiserStorage supportedSerialisers = new MultiSerialiserStorage();

    public void setSerialisers(final List<Content> serialisers) throws GafferCheckedException {
        supportedSerialisers.setSerialisers(serialisers);
    }

    public MultiSerialiser addSerialiser(Content c) throws GafferCheckedException {
        supportedSerialisers.addSerialiser(c);
        return this;
    }

    public List<Content> getSerialisers() {
        return supportedSerialisers.getSerialisers();
    }

    @Override
    public byte[] serialise(final Object object) throws SerialisationException {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            ToBytesSerialiser serialiser = supportedSerialisers.getSerialiserFromValue(object);
            byte[] bytes = serialiser.serialise(object);
            byte key = supportedSerialisers.getKeyFromSerialiser(serialiser);

            stream.write(key);
            stream.write(bytes);
            return stream.toByteArray();
        } catch (SerialisationException e) {
            //re-throw SerialisationException
            throw e;
        } catch (Exception e) {
            //wraps other exceptions.
            throw new SerialisationException(e.getMessage(), e);
        }
    }

    @Override
    public Object deserialise(final byte[] bytes) throws SerialisationException {
        try {
            byte keyByte = bytes[0];
            ToBytesSerialiser serialiser = supportedSerialisers.getSerialiserFromKey(keyByte);
            return serialiser.deserialise(bytes, 1, bytes.length - 1);
        } catch (SerialisationException e) {
            //re-throw SerialisationException
            throw e;
        } catch (Exception e) {
            //wraps other exceptions.
            throw new SerialisationException(e.getMessage(), e);
        }
    }

    @Override
    public Object deserialiseEmpty() throws SerialisationException {
        return null;
    }

    @Override
    public boolean preservesObjectOrdering() {
        return supportedSerialisers.preservesObjectOrdering();
    }

    @Override
    public boolean isConsistent() {
        return supportedSerialisers.isConsistent();
    }


    @Override
    public boolean canHandle(final Class clazz) {
            return supportedSerialisers.canHandle(clazz);
    }
}
