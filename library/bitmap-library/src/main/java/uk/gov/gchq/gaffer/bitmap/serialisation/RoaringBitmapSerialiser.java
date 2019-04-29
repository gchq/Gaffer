/*
 * Copyright 2017-2019 Crown Copyright
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
package uk.gov.gchq.gaffer.bitmap.serialisation;

import org.roaringbitmap.RoaringBitmap;

import uk.gov.gchq.gaffer.bitmap.serialisation.utils.RoaringBitmapUtils;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * A {@link uk.gov.gchq.gaffer.serialisation.Serialiser} implementation for {@link RoaringBitmap}
 * instances.
 */
public class RoaringBitmapSerialiser implements ToBytesSerialiser<RoaringBitmap> {

    private static final long serialVersionUID = 3772387954385745791L;

    @Override
    public boolean canHandle(final Class clazz) {
        return RoaringBitmap.class.equals(clazz);
    }

    @Override
    public byte[] serialise(final RoaringBitmap object) throws SerialisationException {
        final ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        final DataOutputStream out = new DataOutputStream(byteOut);
        try {
            object.serialize(out);
        } catch (final IOException e) {
            throw new SerialisationException(e.getMessage(), e);
        }
        return byteOut.toByteArray();
    }

    @Override
    public RoaringBitmap deserialise(final byte[] allBytes, final int offset, final int length) throws SerialisationException {
        final RoaringBitmap value = new RoaringBitmap();
        final byte[] convertedBytes = RoaringBitmapUtils.upConvertSerialisedForm(allBytes, offset, length);
        final ByteArrayInputStream byteIn = new ByteArrayInputStream(convertedBytes);
        final DataInputStream in = new DataInputStream(byteIn);
        try {
            value.deserialize(in);
        } catch (final IOException e) {
            throw new SerialisationException(e.getMessage(), e);
        }
        return value;
    }

    @Override
    public RoaringBitmap deserialise(final byte[] bytes) throws SerialisationException {
        return deserialise(bytes, 0, bytes.length);
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
    public RoaringBitmap deserialiseEmpty() {
        return new RoaringBitmap();
    }

    @Override
    public byte[] serialiseNull() {
        return new byte[0];
    }

    @Override
    public boolean equals(final Object obj) {
        return this == obj || obj != null && this.getClass() == obj.getClass();
    }

    @Override
    public int hashCode() {
        return RoaringBitmapSerialiser.class.getName().hashCode();
    }
}
