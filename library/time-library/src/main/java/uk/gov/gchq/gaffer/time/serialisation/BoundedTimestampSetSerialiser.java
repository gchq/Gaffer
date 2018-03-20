/*
 * Copyright 2017-2018 Crown Copyright
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

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.sampling.ReservoirLongsUnion;
import org.roaringbitmap.RoaringBitmap;

import uk.gov.gchq.gaffer.bitmap.serialisation.utils.RoaringBitmapUtils;
import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.raw.CompactRawSerialisationUtils;
import uk.gov.gchq.gaffer.time.BoundedTimestampSet;
import uk.gov.gchq.gaffer.time.RBMBackedTimestampSet;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * A {@code BoundedTimestampSetSerialiser} serialises a {@link BoundedTimestampSet} to an array of bytes.
 */
public class BoundedTimestampSetSerialiser implements ToBytesSerialiser<BoundedTimestampSet> {
    private static final long serialVersionUID = 6242522763501581598L;
    private static final byte NOT_FULL = (byte) 0;
    private static final byte SAMPLE = (byte) 1;

    @Override
    public boolean canHandle(final Class clazz) {
        return BoundedTimestampSet.class.equals(clazz);
    }

    @Override
    public byte[] serialise(final BoundedTimestampSet boundedTimestampSet) throws SerialisationException {
        if (null == boundedTimestampSet) {
            return EMPTY_BYTES;
        }
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutputStream dos = new DataOutputStream(baos);
        try {
            dos.write(CompactRawSerialisationUtils.writeLong(boundedTimestampSet.getTimeBucket().ordinal()));
            CompactRawSerialisationUtils.write(boundedTimestampSet.getMaxSize(), dos);
            if (BoundedTimestampSet.State.NOT_FULL.equals(boundedTimestampSet.getState())) {
                dos.write(NOT_FULL);
                boundedTimestampSet.getRbmBackedTimestampSet().getRbm().serialize(dos);
            } else {
                dos.write(SAMPLE);
                final byte[] serialisedRLU = boundedTimestampSet.getReservoirLongsUnion().toByteArray();
                dos.write(serialisedRLU);
            }
        } catch (final IOException e) {
            throw new SerialisationException("Exception writing serialised BoundedTimestampSet to ByteArrayOutputStream",
                    e);
        }
        return baos.toByteArray();
    }

    @Override
    public BoundedTimestampSet deserialise(final byte[] allBytes, final int offset, final int length) throws SerialisationException {
        if (allBytes.length == 0 || length == 0) {
            return null;
        }
        final ByteArrayInputStream bais = new ByteArrayInputStream(allBytes, offset, length);
        final DataInputStream dis = new DataInputStream(bais);
        final int bucketInt = (int) CompactRawSerialisationUtils.read(dis);
        final CommonTimeUtil.TimeBucket bucket = CommonTimeUtil.TimeBucket.values()[bucketInt];
        final int maxSize = (int) CompactRawSerialisationUtils.read(dis);
        final BoundedTimestampSet boundedTimestampSet = new BoundedTimestampSet(bucket, maxSize);
        try {
            final byte state = dis.readByte();
            if (NOT_FULL == state) {
                final RBMBackedTimestampSet rbmBackedTimestampSet = new RBMBackedTimestampSet(bucket);
                final RoaringBitmap rbm = new RoaringBitmap();
                final byte[] serialisedRBM = new byte[bais.available()];
                if (-1 == dis.read(serialisedRBM)) {
                    throw new SerialisationException("Unexpected end of stream when reading serialised RoaringBitmap");
                }
                final byte[] convertedBytes = RoaringBitmapUtils.upConvertSerialisedForm(serialisedRBM, 0, serialisedRBM.length);
                final ByteArrayInputStream baisConvertedBytes = new ByteArrayInputStream(convertedBytes);
                final DataInputStream disConvertedBytes = new DataInputStream(baisConvertedBytes);
                rbm.deserialize(disConvertedBytes);
                rbmBackedTimestampSet.setRbm(rbm);
                boundedTimestampSet.setRbmBackedTimestampSet(rbmBackedTimestampSet);
            } else if (SAMPLE == state) {
                final byte[] serialisedRLU = new byte[dis.available()];
                if (-1 == dis.read(serialisedRLU)) {
                    throw new SerialisationException("Unexpected end of stream when reading serialised ReservoirLongsUnion");
                }
                final ReservoirLongsUnion reservoirLongsUnion = ReservoirLongsUnion.heapify(WritableMemory.wrap(serialisedRLU));
                boundedTimestampSet.setReservoirLongsUnion(reservoirLongsUnion);
            } else {
                throw new SerialisationException("Unexpected byte indicating the state: expected " + NOT_FULL + " or "
                        + SAMPLE + ", got " + state);
            }
        } catch (final IOException e) {
            throw new SerialisationException("IOException deserialising BoundedTimestampSet from byte array", e);
        }
        return boundedTimestampSet;
    }

    @Override
    public BoundedTimestampSet deserialise(final byte[] bytes) throws SerialisationException {
        return deserialise(bytes, 0, bytes.length);
    }

    @Override
    public BoundedTimestampSet deserialiseEmpty() throws SerialisationException {
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
}
