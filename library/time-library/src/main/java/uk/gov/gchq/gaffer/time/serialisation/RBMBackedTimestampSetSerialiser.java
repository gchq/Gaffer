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

import org.roaringbitmap.RoaringBitmap;
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
 *
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
    public RBMBackedTimestampSet deserialise(final byte[] bytes) throws SerialisationException {
        if (0 == bytes.length) {
            return null;
        }
        final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        final DataInputStream dis = new DataInputStream(bais);
        final int bucketInt = (int) CompactRawSerialisationUtils.read(dis);
        final TimeBucket bucket = TimeBucket.values()[bucketInt];
        final RBMBackedTimestampSet rbmBackedTimestampSet = new RBMBackedTimestampSet(bucket);
        final RoaringBitmap rbm = new RoaringBitmap();
        try {
            rbm.deserialize(dis);
        } catch (final IOException e) {
            throw new SerialisationException("IOException deserialising RoaringBitmap from byte array", e);
        }
        rbmBackedTimestampSet.setRbm(rbm);
        return rbmBackedTimestampSet;
    }

    @Override
    public RBMBackedTimestampSet deserialiseEmpty() throws SerialisationException {
        return null;
    }

    @Override
    public boolean preservesObjectOrdering() {
        return false;
    }
}
