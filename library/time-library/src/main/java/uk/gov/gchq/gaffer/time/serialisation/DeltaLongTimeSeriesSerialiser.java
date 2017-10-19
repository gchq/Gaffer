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
package uk.gov.gchq.gaffer.time.serialisation;

import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil.TimeBucket;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.CompactRawSerialisationUtils;
import uk.gov.gchq.gaffer.time.LongTimeSeries;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.SortedMap;

/**
 * Serialises a {@link LongTimeSeries} by writing out the deltas between
 * consecutive values in the timeseries. The same approach is used, independently,
 * for both the timestamps and the values. This should store the time series
 * compactly when there is some regularity in the spacing of the keys or values.
 *
 * <p>If the values of the time series are extreme, i.e. greater than half of
 * <code>Long.MAX_VALUE</code> in absolute size, then the deltas might overflow.
 * In this case, a simpler serialisation is used where the timestamps and values
 * are simply written out directly.
 */
public class DeltaLongTimeSeriesSerialiser implements ToBytesSerialiser<LongTimeSeries> {
    private static final long HALF_MAX_VALUE = Long.MAX_VALUE / 2;
    private static final long serialVersionUID = -5820977643949438174L;

    @Override
    public boolean canHandle(final Class clazz) {
        return LongTimeSeries.class.equals(clazz);
    }

    @Override
    public byte[] serialise(final LongTimeSeries timeSeries) throws SerialisationException {
        if (null == timeSeries) {
            return EMPTY_BYTES;
        }
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutputStream dos = new DataOutputStream(baos);
        final Mode mode = calculateMode(timeSeries);
        final SortedMap<Instant, Long> timeseriesMap = timeSeries.getTimeSeries();
        try {
            dos.write(CompactRawSerialisationUtils.writeLong(timeSeries.getTimeBucket().ordinal()));
            dos.write(CompactRawSerialisationUtils.writeLong(timeseriesMap.size()));
            boolean deltaMode = mode == Mode.DELTA;
            dos.writeBoolean(deltaMode);
            if (deltaMode) {
                deltaSerialise(timeseriesMap, dos);
            } else {
                defaultSerialise(timeseriesMap, dos);
            }
        } catch (final IOException e) {
            throw new SerialisationException("Exception writing serialised LongTimeSeries to ByteArrayOutputStream",
                    e);
        }
        return baos.toByteArray();
    }

    @Override
    public LongTimeSeries deserialise(final byte[] allBytes, final int offset, final int length) throws SerialisationException {
        if (allBytes.length == 0 || length == 0) {
            return null;
        }
        final ByteArrayInputStream bais = new ByteArrayInputStream(allBytes, offset, length);
        final DataInputStream dis = new DataInputStream(bais);
        final int bucketInt = (int) CompactRawSerialisationUtils.read(dis);
        final TimeBucket bucket = TimeBucket.values()[bucketInt];
        final int numEntries = (int) CompactRawSerialisationUtils.read(dis);
        final LongTimeSeries timeSeries = new LongTimeSeries(bucket);
        try {
            final boolean deltaMode = dis.readBoolean();
            if (deltaMode) {
                deltaDeserialise(timeSeries, numEntries, dis);
            } else {
                defaultDeserialise(timeSeries, numEntries, dis);
            }
        } catch (final IOException e) {
            throw new SerialisationException("IOException reading boolean", e);
        }
        return timeSeries;
    }

    @Override
    public LongTimeSeries deserialise(final byte[] bytes) throws SerialisationException {
        return deserialise(bytes, 0, bytes.length);
    }

    @Override
    public LongTimeSeries deserialiseEmpty() throws SerialisationException {
        return null;
    }

    @Override
    public boolean preservesObjectOrdering() {
        return false;
    }

    @Override
    public boolean isConsistent() {
        return true;
    }

    private enum Mode {
        DELTA, LITERAL
    }

    private void deltaSerialise(final SortedMap<Instant, Long> timeSeriesMap, final DataOutputStream dos) throws SerialisationException {
        long previousKey = 0L;
        long previousValue = 0L;
        for (final Map.Entry<Instant, Long> entry : timeSeriesMap.entrySet()) {
            final long currentKey = entry.getKey().toEpochMilli();
            CompactRawSerialisationUtils.write(currentKey - previousKey, dos);
            previousKey = currentKey;
            final long currentValue = entry.getValue();
            CompactRawSerialisationUtils.write(currentValue - previousValue, dos);
            previousValue = currentValue;
        }
    }

    private void deltaDeserialise(final LongTimeSeries timeSeries,
                                  final int numEntries,
                                  final DataInputStream dis) throws SerialisationException {
        long previousKey = 0L;
        long previousValue = 0L;
        for (int i = 0; i < numEntries; i++) {
            final long currentKey = CompactRawSerialisationUtils.read(dis);
            final long time = currentKey + previousKey;
            final long currentValue = CompactRawSerialisationUtils.read(dis);
            final long value = currentValue + previousValue;
            timeSeries.upsert(Instant.ofEpochMilli(time), value);
            previousKey = time;
            previousValue = value;
        }
    }

    private void defaultSerialise(final Map<Instant, Long> timeSeriesMap, final DataOutputStream dos) throws SerialisationException {
        for (final Map.Entry<Instant, Long> entry : timeSeriesMap.entrySet()) {
            final long currentKey = entry.getKey().toEpochMilli();
            CompactRawSerialisationUtils.write(currentKey, dos);
            final long currentValue = entry.getValue();
            CompactRawSerialisationUtils.write(currentValue, dos);
        }
    }

    private void defaultDeserialise(final LongTimeSeries timeSeries,
                                    final int numEntries,
                                    final DataInputStream dis) throws SerialisationException {
        for (int i = 0; i < numEntries; i++) {
            final long currentKey = CompactRawSerialisationUtils.read(dis);
            final long currentValue = CompactRawSerialisationUtils.read(dis);
            timeSeries.upsert(Instant.ofEpochMilli(currentKey), currentValue);
        }
    }

    private static Mode calculateMode(final LongTimeSeries timeSeries) {
        final boolean noneMatch = timeSeries
                .getTimeSeries()
                .entrySet()
                .stream()
                .noneMatch(e -> e.getKey().toEpochMilli() < -HALF_MAX_VALUE
                        || e.getKey().toEpochMilli() > HALF_MAX_VALUE
                        || e.getValue() < -HALF_MAX_VALUE
                        || e.getValue() > HALF_MAX_VALUE);
        return noneMatch ? Mode.DELTA : Mode.LITERAL;
    }
}
