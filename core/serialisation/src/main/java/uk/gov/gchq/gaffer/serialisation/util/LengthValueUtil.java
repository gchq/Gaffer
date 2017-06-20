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
package uk.gov.gchq.gaffer.serialisation.util;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.CompactRawSerialisationUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

public final class LengthValueUtil {

    private LengthValueUtil() { }

    public static ByteArrayOutputStream createByteArray() {
        return new ByteArrayOutputStream();
    }

    public static ByteArrayOutputStream appendLengthValueFromObjectToByteStream(final ByteArrayOutputStream byteOut, final ToBytesSerialiser serialiser, final Object object) throws SerialisationException {
        return appendLengthValueFromBytesToByteStream(byteOut, serialiser.serialise(object));
    }

    public static ByteArrayOutputStream appendLengthValueFromBytesToByteStream(final ByteArrayOutputStream byteOut, final byte[] serialisedObject) throws SerialisationException {
        CompactRawSerialisationUtils.write(serialisedObject.length, byteOut);
        try {
            byteOut.write(serialisedObject);
        } catch (IOException e) {
            throw new SerialisationException(e.getMessage(), e);
        }
        return byteOut;
    }

    public static <T> ObjectCarriage<T> deserialiseNextObject(final ToBytesSerialiser<T> serialiser, final int currentCarriage, final byte[] bytes) throws SerialisationException {
        int rtn = currentCarriage;
        int numBytesForLength = CompactRawSerialisationUtils.decodeVIntSize(bytes[rtn]);
        int currentPropLength = getCurrentPropLength(bytes, rtn, numBytesForLength);
        int from = rtn += numBytesForLength;
        int to = rtn += currentPropLength;
        T object = serialiser.deserialise(Arrays.copyOfRange(bytes, from, to));
        return new ObjectCarriage<T>(object, rtn);
    }

    private static int getCurrentPropLength(final byte[] bytes, final int pos, final int numBytesForLength) throws SerialisationException {
        final byte[] length = new byte[numBytesForLength];
        System.arraycopy(bytes, pos, length, 0, numBytesForLength);
        return (int) CompactRawSerialisationUtils.readLong(length);
    }


    public static class ObjectCarriage<T> {
        private T object;
        private int carriage;

        ObjectCarriage(final T object, final int carriage) {
            this.object = object;
            this.carriage = carriage;
        }

        public T getObject() {
            return object;
        }

        public void setObject(final T object) {
            this.object = object;
        }

        public int getCarriage() {
            return carriage;
        }

        public void setCarriage(final int carriage) {
            this.carriage = carriage;
        }
    }

    public static class LengthValueBuilder {

        ByteArrayOutputStream byteOut = LengthValueUtil.createByteArray();

        public LengthValueBuilder appendLengthValueFromObjectToByteStream(final ToBytesSerialiser serialiser, final Object object) throws SerialisationException {
            LengthValueUtil.appendLengthValueFromObjectToByteStream(byteOut, serialiser, object);
                return this;
        }

        public LengthValueBuilder appendLengthValueFromBytesToByteStream(final byte[] serialisedObject) throws SerialisationException {
            LengthValueUtil.appendLengthValueFromBytesToByteStream(byteOut, serialisedObject);
            return this;
        }

        public byte[] toArray() {
            return byteOut.toByteArray();
        }

    }

}
