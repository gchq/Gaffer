/*
 * Copyright 2016-2018 Crown Copyright
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * Utility methods for serialising objects to length-value byte arrays.
 */
public abstract class LengthValueBytesSerialiserUtil {
    private static final byte[] EMPTY_BYTES = new byte[0];

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
        } catch (final IOException e) {
            throw new SerialisationException(e.getMessage(), e);
        }
        return byteOut;
    }

    public static <T> byte[] serialise(final ToBytesSerialiser<T> serialiser, final T value)
            throws SerialisationException {
        final byte[] valueBytes = getValueBytes(serialiser, value);
        return serialise(valueBytes);
    }

    public static <T> void serialise(final ToBytesSerialiser<T> serialiser, final T value, final ByteArrayOutputStream out)
            throws SerialisationException {
        final byte[] valueBytes = getValueBytes(serialiser, value);
        serialise(valueBytes, out);
    }

    public static byte[] serialise(final byte[] valueBytes) throws SerialisationException {
        try (final ByteArrayOutputStream byteStream = new ByteArrayOutputStream()) {
            serialise(valueBytes, byteStream);
            return byteStream.toByteArray();
        } catch (final IOException e) {
            throw new SerialisationException("Unable to write bytes to output stream", e);
        }
    }

    public static void serialise(final byte[] valueBytes, final ByteArrayOutputStream out)
            throws SerialisationException {
        if (null == valueBytes || 0 == valueBytes.length) {
            CompactRawSerialisationUtils.write(0, out);
        } else {
            CompactRawSerialisationUtils.write(valueBytes.length, out);
            try {
                out.write(valueBytes);
            } catch (final IOException e) {
                throw new SerialisationException("Unable to write bytes to output stream", e);
            }
        }
    }

    public static <T> T deserialise(final ToBytesSerialiser<T> serialiser, final byte[] allBytes) throws SerialisationException {
        return deserialise(serialiser, allBytes, 0);
    }

    public static <T> T deserialise(final ToBytesSerialiser<T> serialiser, final byte[] allBytes, final int delimiter) throws SerialisationException {
        return getValue(serialiser, deserialise(allBytes, delimiter));
    }

    public static <T> T deserialise(final ToBytesSerialiser<T> serialiser, final byte[] allBytes, final int[] delimiterWrapper) throws SerialisationException {
        return getValue(serialiser, deserialise(allBytes, delimiterWrapper));
    }

    public static byte[] deserialise(final byte[] allBytes, final int[] delimiterWrapper) throws SerialisationException {
        if (1 != delimiterWrapper.length) {
            throw new IllegalArgumentException("Delimiter wrapper must always be a int array of length 1 containing the delimiter");
        }

        final int lengthSize = getLengthSize(allBytes, delimiterWrapper[0]);
        int valueSize = getValueSize(allBytes, lengthSize, delimiterWrapper[0]);
        final byte[] valueBytes = deserialise(allBytes, lengthSize, valueSize, delimiterWrapper[0]);
        delimiterWrapper[0] = getNextDelimiter(lengthSize, valueSize, delimiterWrapper[0]);

        return valueBytes;
    }

    public static byte[] deserialise(final byte[] allBytes) throws SerialisationException {
        return deserialise(allBytes, 0);
    }

    public static byte[] deserialise(final byte[] allBytes, final int delimiter) throws SerialisationException {
        if (null == allBytes || 0 == allBytes.length) {
            return new byte[0];
        }

        final int lengthSize = getLengthSize(allBytes, delimiter);
        int valueSize = getValueSize(allBytes, lengthSize, delimiter);
        return deserialise(allBytes, lengthSize, valueSize, delimiter);
    }

    public static byte[] deserialise(final byte[] allBytes, final int lengthSize, final int valueSize, final int delimiter) throws SerialisationException {
        if (null == allBytes || 0 == allBytes.length) {
            return new byte[0];
        }

        return Arrays.copyOfRange(allBytes, delimiter + lengthSize, delimiter + lengthSize + valueSize);
    }

    public static int getLengthSize(final byte[] allBytes, final int delimiter) throws SerialisationException {
        return CompactRawSerialisationUtils.decodeVIntSize(allBytes[delimiter]);
    }

    public static int getValueSize(final byte[] allBytes, final int delimiter) throws SerialisationException {
        return getValueSize(allBytes, getLengthSize(allBytes, delimiter), delimiter);
    }

    public static int getValueSize(final byte[] allBytes, final int lengthSize, final int delimiter) throws SerialisationException {
        try (final ByteArrayInputStream input = new ByteArrayInputStream(allBytes, delimiter, lengthSize)) {
            return (int) CompactRawSerialisationUtils.read(input);
        } catch (final IOException e) {
            throw new SerialisationException("Exception reading length of property", e);
        }
    }

    public static int getNextDelimiter(final byte[] allBytes, final int delimiter) throws SerialisationException {
        final int lengthSize = getLengthSize(allBytes, delimiter);
        final int valueSize = getValueSize(allBytes, lengthSize, delimiter);
        return getNextDelimiter(lengthSize, valueSize, delimiter);
    }

    public static int getNextDelimiter(final byte[] allBytes, final byte[] valueBytes, final int lastDelimiter) {
        return getNextDelimiter(allBytes, valueBytes.length, lastDelimiter);
    }

    public static int getNextDelimiter(final byte[] allBytes, final int valueSize, final int lastDelimiter) {
        return getNextDelimiter(CompactRawSerialisationUtils.decodeVIntSize(allBytes[lastDelimiter]), valueSize, lastDelimiter);
    }

    public static int getNextDelimiter(final int lengthSize, final int valueSize, final int lastDelimiter) {
        return lastDelimiter + lengthSize + valueSize;
    }

    public static <T> byte[] getValueBytes(final ToBytesSerialiser<T> serialiser, final T value) throws SerialisationException {
        final byte[] valueBytes;
        if (null == serialiser) {
            valueBytes = EMPTY_BYTES;
        } else if (null == value) {
            valueBytes = serialiser.serialiseNull();
        } else {
            valueBytes = serialiser.serialise(value);
        }
        return valueBytes;
    }

    private static <T> T getValue(final ToBytesSerialiser<T> serialiser, final byte[] valueBytes) throws SerialisationException {
        if (0 == valueBytes.length) {
            return serialiser.deserialiseEmpty();
        }
        return serialiser.deserialise(valueBytes);
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

        ByteArrayOutputStream byteOut = LengthValueBytesSerialiserUtil.createByteArray();

        public LengthValueBuilder appendLengthValueFromObjectToByteStream(final ToBytesSerialiser serialiser, final Object object) throws SerialisationException {
            LengthValueBytesSerialiserUtil.appendLengthValueFromObjectToByteStream(byteOut, serialiser, object);
            return this;
        }

        public LengthValueBuilder appendLengthValueFromBytesToByteStream(final byte[] serialisedObject) throws SerialisationException {
            LengthValueBytesSerialiserUtil.appendLengthValueFromBytesToByteStream(byteOut, serialisedObject);
            return this;
        }

        public byte[] toArray() {
            return byteOut.toByteArray();
        }

    }
}

