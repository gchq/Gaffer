/*
 * Copyright 2017 Crown Copyright
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
import uk.gov.gchq.gaffer.bitmap.types.MapOfBitmaps;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.CompactRawSerialisationUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class StringKeyedMapOfBitmapsSerialiser implements ToBytesSerialiser<MapOfBitmaps> {

    private static final RoaringBitmapSerialiser BITMAP_SERIALISER = new RoaringBitmapSerialiser();
    private static final StringSerialiser STRING_SERIALISER = new StringSerialiser();

    @Override
    public boolean canHandle(final Class clazz) {
        return MapOfBitmaps.class.equals(clazz);
    }

    @Override
    public byte[] serialise(final MapOfBitmaps object) throws SerialisationException {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        try {
            for (final Map.Entry<Object, RoaringBitmap> entry : object.entrySet()) {
                final Object key = entry.getKey();
                if (!key.getClass().equals(String.class)) {
                    throw new SerialisationException("" +
                            "Key in MapOfBitmaps was not of expected type, expected: class java.lang.String but was " + key.getClass());
                }
                final byte[] k = STRING_SERIALISER.serialise((String) key);
                final byte[] v = BITMAP_SERIALISER.serialise(entry.getValue());
                CompactRawSerialisationUtils.write(k.length, byteOut);
                byteOut.write(k);
                CompactRawSerialisationUtils.write(v.length, byteOut);
                byteOut.write(v);
            }
        } catch (IOException e) {
            throw new SerialisationException(e.getMessage(), e);
        }
        return byteOut.toByteArray();
    }

    @Override
    public MapOfBitmaps deserialise(final byte[] bytes) throws SerialisationException {
        MapOfBitmaps mapOfBitmaps = new MapOfBitmaps();
        final int arrayLength = bytes.length;
        int carriage = 0;
        while (carriage < arrayLength) {
            int rtn = carriage;
            int numBytesForLength = CompactRawSerialisationUtils.decodeVIntSize(bytes[rtn]);
            int currentPropLength = getCurrentPropLength(bytes, rtn, numBytesForLength);
            int from = rtn += numBytesForLength;
            int to = rtn += currentPropLength;
            String key = STRING_SERIALISER.deserialise(Arrays.copyOfRange(bytes, from, to));
            numBytesForLength = CompactRawSerialisationUtils.decodeVIntSize(bytes[rtn]);
            currentPropLength = getCurrentPropLength(bytes, rtn, numBytesForLength);
            from = rtn += numBytesForLength;
            to = rtn += currentPropLength;
            RoaringBitmap value = BITMAP_SERIALISER.deserialise(Arrays.copyOfRange(bytes, from, to));
            mapOfBitmaps.put(key, value);
            carriage = rtn;
        }
        return mapOfBitmaps;
    }

    @Override
    public MapOfBitmaps deserialiseEmpty() throws SerialisationException {
        return null;
    }

    private int getCurrentPropLength(final byte[] bytes, final int pos, final int numBytesForLength) throws SerialisationException {
        final byte[] length = new byte[numBytesForLength];
        System.arraycopy(bytes, pos, length, 0, numBytesForLength);
        return (int) CompactRawSerialisationUtils.readLong(length);
    }

    @Override
    public boolean preservesObjectOrdering() {
        return false;
    }

    @Override
    public byte[] serialiseNull() {
        return new byte[0];
    }
}
