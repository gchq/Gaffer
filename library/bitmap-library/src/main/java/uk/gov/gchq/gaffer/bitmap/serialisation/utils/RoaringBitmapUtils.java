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
package uk.gov.gchq.gaffer.bitmap.serialisation.utils;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Contains a method for converting version 0.1.5 serialised RoaringBitmaps into
 * version 0.4.0-0.6.35 compatible forms.
 */
public final class RoaringBitmapUtils {
    private static final int BITMAP_CONTAINER_SIZE = (1 << 16) / 8;
    private static final int MAX_ARRAY_CONTAINER_SIZE = 4096;
    private static final short VERSION_ZERO_ONE_FIVE_TO_ZERO_THREE_SEVEN_SERIAL_COOKIE = 12345;
    private static final short VERSION_ZERO_FOUR_ZERO_TO_SIX_THRIRTY_FIVE_NO_RUNCONTAINER_COOKIE = 12346;
    private static final short VERSION_ZERO_FIVE_ZERO_TO_SIX_THIRTY_FIVE_COOKIE = 12347;
    private static final byte[] VERSION_ZERO_FOUR_ZERO_TO_SIX_THIRTY_FIVE_NO_RUNCONTAINER_COOKIE_BYTES = ByteBuffer.allocate(4).putInt(Integer.reverseBytes(VERSION_ZERO_FOUR_ZERO_TO_SIX_THRIRTY_FIVE_NO_RUNCONTAINER_COOKIE)).array();

    private RoaringBitmapUtils() {

    }

    public static byte[] upConvertSerialisedForm(final byte[] serialisedBitmap) throws SerialisationException {
        DataInputStream input = new DataInputStream(new ByteArrayInputStream(serialisedBitmap));
        int cookie;
        try {
            cookie = Integer.reverseBytes(input.readInt());
        } catch (final IOException e) {
            throw new SerialisationException("I failed to read the bitmap version cookie", e);
        }

        if (cookie == VERSION_ZERO_ONE_FIVE_TO_ZERO_THREE_SEVEN_SERIAL_COOKIE) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream(128);
                DataOutputStream out = new DataOutputStream(baos);
                out.write(VERSION_ZERO_FOUR_ZERO_TO_SIX_THIRTY_FIVE_NO_RUNCONTAINER_COOKIE_BYTES);
                int sizeInt = input.readInt();
                int size = Integer.reverseBytes(sizeInt);

                //Doesn't need to be reversed (already read as reversed)
                out.writeInt(sizeInt);
                int startOffSet = 4 + 4 + 4 * size + 4 * size;

                //Need to extract the cardinalities to calculate the offsets
                //Bitmap containers are a fixed size (BITMAP_CONTAINER_SIZE) and are used when the cardinality is greater than 4096
                //Array containers are variable size (cardinality * 2)
                int[] cardinalities = new int[size];
                for (int i = 0; i < size; ++i) {

                    //Read and write the key (Don't need to use this just copy it across)
                    out.writeShort(input.readShort());

                    //Get the cardinality and store it for calculating offsets
                    short cardShort = input.readShort();
                    cardinalities[i] = 1 + (0xFFFF & Short.reverseBytes(cardShort));

                    //Doesn't need reversing (already read as reversed)
                    out.writeShort(cardShort);
                }
                int currentOffSet = startOffSet;
                for (int i = 0; i < size; ++i) {
                    out.writeInt(Integer.reverseBytes(currentOffSet));
                    if (cardinalities[i] > MAX_ARRAY_CONTAINER_SIZE) {

                        //It's a bitmap container
                        currentOffSet += BITMAP_CONTAINER_SIZE;
                    } else {

                        //It's an array container
                        currentOffSet += (cardinalities[i] * 2);
                    }
                }
                int expectedNumContainerBytes = currentOffSet - startOffSet;

                //Write out all the container data
                int numContainerBytes = 0;
                int b;
                while ((b = input.read()) != -1) {
                    out.write(b);
                    ++numContainerBytes;
                }
                out.flush();
                if (numContainerBytes != expectedNumContainerBytes) {
                    throw new SerialisationException("I failed to convert roaring bitmap from pre 0.4.0 version");
                }
                return baos.toByteArray();
            } catch (SerialisationException e) {
                throw (e);
            } catch (final IOException e) {
                throw new SerialisationException("IOException: I failed to convert roaring bitmap from pre 0.4.0 version", e);
            }
        } else if (cookie == VERSION_ZERO_FOUR_ZERO_TO_SIX_THRIRTY_FIVE_NO_RUNCONTAINER_COOKIE || (cookie & 0xFFFF) == VERSION_ZERO_FIVE_ZERO_TO_SIX_THIRTY_FIVE_COOKIE) {
            return serialisedBitmap;
        } else {
            throw new SerialisationException("I failed to find a known roaring bitmap cookie (cookie = " + cookie + ")");
        }
    }
}
