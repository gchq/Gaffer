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
package uk.gov.gchq.gaffer.time.serialisation;

import org.roaringbitmap.RoaringBitmap;

import uk.gov.gchq.gaffer.bitmap.serialisation.utils.RoaringBitmapUtils;
import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil.TimeBucket;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.CompactRawSerialisationUtils;
import uk.gov.gchq.gaffer.time.RBMBackedTimestampSet;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * A {@code RBMBackedTimestampSetSerialiser} serialises a {@link RBMBackedTimestampSet} to an array of bytes.
 * <p>The following figures illustrate the serialised size of some {@link RBMBackedTimestampSet}s with different
 * numbers of timestamps added and different time buckets:
 * <ul>
 * <li> When the time bucket is a minute and 100 random minutes from a single day are added then the serialised
 * size is approximately 210 bytes.
 * <li> When the time bucket is a minute and every minute in a single year is added then the serialised size is
 * approximately 73000 bytes.
 * <li> When the time bucket is a second and every second in a single year is added then the serialised size is
 * approximately 4,000,000 bytes.
 * </ul>
 */
public class RBMBackedTimestampSetSerialiser implements ToBytesSerialiser<RBMBackedTimestampSet> {
    private static final long serialVersionUID = -5820977643949438174L;

    @Override
    public boolean canHandle(final Class clazz) {
        return RBMBackedTimestampSet.class.equals(clazz);
    }

    @Override
    public byte[] serialise(final RBMBackedTimestampSet rbmBackedTimestampSet) throws SerialisationException {
        if (null == rbmBackedTimestampSet) {
            return EMPTY_BYTES;
        }
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutputStream dos = new DataOutputStream(baos);
        try {
            dos.write(CompactRawSerialisationUtils.writeLong(rbmBackedTimestampSet.getTimeBucket().ordinal()));
            rbmBackedTimestampSet.getRbm().serialize(dos);
        } catch (final IOException e) {
            throw new SerialisationException("Exception writing serialised RBMBackedTimestampSet to ByteArrayOutputStream",
                    e);
        }
        return baos.toByteArray();
    }

    @Override
    public RBMBackedTimestampSet deserialise(final byte[] allBytes, final int offset, final int length) throws SerialisationException {
        if (allBytes.length == 0 || length == 0) {
            return null;
        }
        final int bucketInt = (int) CompactRawSerialisationUtils.readLong(allBytes, offset);
        final int numBytesForInt = CompactRawSerialisationUtils.decodeVIntSize(allBytes[offset]);
        final TimeBucket bucket = TimeBucket.values()[bucketInt];
        final RBMBackedTimestampSet rbmBackedTimestampSet = new RBMBackedTimestampSet(bucket);
        final RoaringBitmap rbm = new RoaringBitmap();
        try {
            // Deal with different versions of RoaringBitmap
            final byte[] convertedBytes = RoaringBitmapUtils.upConvertSerialisedForm(allBytes, offset + numBytesForInt, length - numBytesForInt);
            final ByteArrayInputStream baisConvertedBytes = new ByteArrayInputStream(convertedBytes);
            final DataInputStream disConvertedBytes = new DataInputStream(baisConvertedBytes);
            rbm.deserialize(disConvertedBytes);
        } catch (final IOException e) {
            throw new SerialisationException("IOException deserialising RoaringBitmap from byte array", e);
        }
        rbmBackedTimestampSet.setRbm(rbm);
        return rbmBackedTimestampSet;
    }

    @Override
    public RBMBackedTimestampSet deserialise(final byte[] bytes) throws SerialisationException {
        return deserialise(bytes, 0, bytes.length);
    }

    @Override
    public RBMBackedTimestampSet deserialiseEmpty() throws SerialisationException {
        return null;
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
        return RBMBackedTimestampSetSerialiser.class.getName().hashCode();
    }
}
