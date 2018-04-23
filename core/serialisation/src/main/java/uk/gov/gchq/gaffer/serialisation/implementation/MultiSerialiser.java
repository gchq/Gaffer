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

import java.io.ByteArrayOutputStream;
import java.util.List;

/**
 * This class is used to serialise and deserialise multiple object types.
 * <p>
 * The serialiser used is stored at the first byte of the serial byte[].
 * This mean encoding can only go up to the size of a byte (256),
 * however nesting MultiSerialiser you can increase encoding beyond the first byte to have more Serialisers.
 * <p>
 * {@code new byte[]{256,1,<byte values>}}
 * <br>
 * 256 could encode to a nested MultiSerialiser and now the second byte is also used for encoding extra serialisers.
 */
public class MultiSerialiser implements ToBytesSerialiser<Object> {
    private static final long serialVersionUID = 8206706506883696003L;
    public static final String ERROR_PAIR = "setSupportedSerialisers requires pair's of classes e.g. setSupportedSerialisers(StringSerialiser.class, String.class)";
    public static final String ERROR_PAIR_ORDER = "setSupportedSerialisers requires  pair's of classes in the order e.g (Class<? extends ToBytesSerialiser> serialiserClass, Class supportedClass) ";
    private MultiSerialiserStorage supportedSerialisers = new MultiSerialiserStorage();

    public void setSerialisers(final List<Contents> serialisers) throws GafferCheckedException {
        if (null != serialisers) {
            for (Contents serialiser : serialisers) {
                supportedSerialisers.put(serialiser.getKey(), serialiser.getSerialiser(), serialiser.getValueClass());
            }
        }
    }

    public List<Contents> getSerialisers() {
        return null;
    }

    @Override
    public byte[] serialise(final Object object) throws SerialisationException {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            ToBytesSerialiser serialiser = supportedSerialisers.getSerialiserFromValue(object);
            byte[] bytes = serialiser.serialise(object);
            byte key = supportedSerialisers.getKeyFromSerialiser(serialiser.getClass());

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
        try {
            return supportedSerialisers.canHandle(clazz);
        } catch (GafferCheckedException e) {
            throw new RuntimeException("MultiSerialiser unable to recover from canHandle()", e);
        }
    }

    public static class Contents {
        public Contents() {
        }

        public Contents(final byte key, final Class<? extends ToBytesSerialiser> serialiser, final Class valueClass) {
            this();
            this.key = key;
            this.serialiser = serialiser;
            this.valueClass = valueClass;
        }

        private byte key;
        private Class<? extends ToBytesSerialiser> serialiser;
        private Class valueClass;


        public byte getKey() {
            return key;
        }

        public Contents key(final byte key) {
            this.key = key;
            return this;
        }

        public Class getValueClass() {
            return valueClass;
        }

        public Contents valueClass(final Class valueClass) {
            this.valueClass = valueClass;
            return this;
        }

        public Class<? extends ToBytesSerialiser> getSerialiser() {
            return serialiser;
        }

        public Contents serialiser(final Class<? extends ToBytesSerialiser> serialiser) {
            this.serialiser = serialiser;
            return this;
        }
    }


}
