/*
 * Copyright 2018-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.serialisation.implementation;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.core.exception.GafferCheckedException;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.MultiSerialiserStorage.SerialiserDetail;

import java.io.ByteArrayOutputStream;
import java.util.List;

/**
 * This class is used to serialise and deserialise multiple object types.
 * This overcomes the limitation of a graph schema requiring all vertex types
 * to use the same serialiser.
 * <p>
 * The serialiser used is stored at the first byte of the serialised byte[]
 * The maximum number of different serialiser keys that can be used is limited to 256 (the size of a byte).
 * <pre>
 * byte array [42,*,*,*,*,*]
 *             ^
 *             ^
 *             serialiser byte key
 * </pre>
 * When multiple serialisers are used that operate on the same value class then
 * the last serialiser in the list will be used for serialisation; this happens
 * regardless of the key's natural-ordering. In the example Integers will be serialised with no.8.
 * <br>
 * For deserialising the correct serialiser is always chosen based on the byte key at the start of the byte[].
 * <p>
 * In the below example, the MultiSerialiser has 3 Integer serialisers.
 * This has backwards compatibility to read and deserialise each byte array regardless of the keyed serialiser used at the time of writing, as long as its on the classpath.
 * However when re-serialising the Integer object, only the last serialiser will be used (key no.8, CompactRawIntegerSerialiser)
 * <br>
 * This allows the MultiSerialiser to be updated with improvements and maintain backwards compatibility. This also allows serialisation to be
 * updated before a Serialiser is deprecated and removed.
 * <pre>
 *     Json
 *     {
 *       "serialisers" : [ {
 *         "key" : 0,
 *         "serialiser" : {
 *           "class" : "uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser",
 *           "charset" : "UTF-8"
 *         },
 *         "valueClass" : "java.lang.String"
 *       }, {
 *         "key" : 7,
 *         "serialiser" : {
 *           "class" : "old.deprecated.IntegerSerialiser",
 *           "charset" : "ISO-8859-1"
 *         },
 *         "valueClass" : "java.lang.Integer"
 *       }, {
 *         "key" : 24,
 *         "serialiser" : {
 *           "class" : "uk.gov.gchq.gaffer.serialisation.implementation.ordered.OrderedIntegerSerialiser"
 *         },
 *         "valueClass" : "java.lang.Integer"
 *       }, {
 *         "key" : 8,
 *         "serialiser" : {
 *           "class" : "uk.gov.gchq.gaffer.serialisation.implementation.raw.CompactRawIntegerSerialiser"
 *         },
 *         "valueClass" : "java.lang.Integer"
 *       } ]
 *     }
 *     </pre>
 */
public class MultiSerialiser implements ToBytesSerialiser<Object> {
    private static final long serialVersionUID = 8206706506883696003L;
    @SuppressFBWarnings(value = "SE_BAD_FIELD", justification = "Investigate if MultiSerialiserStorage needs to implement Serializable")
    private final MultiSerialiserStorage supportedSerialisers = new MultiSerialiserStorage();

    public void setSerialisers(final List<SerialiserDetail> serialisers) throws GafferCheckedException {
        supportedSerialisers.setSerialiserDetails(serialisers);
    }

    public MultiSerialiser addSerialiser(final byte key, final ToBytesSerialiser serialiser, final Class aClass) throws GafferCheckedException {
        supportedSerialisers.addSerialiserDetails(key, serialiser, aClass);
        return this;
    }

    public List<SerialiserDetail> getSerialisers() {
        return supportedSerialisers.getSerialiserDetails();
    }

    @Override
    public byte[] serialise(final Object object) throws SerialisationException {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            byte key = supportedSerialisers.getKeyFromValue(object);
            byte[] bytes = nullCheck(supportedSerialisers.getSerialiserFromKey(key)).serialise(object);

            stream.write(key);
            stream.write(bytes);
            return stream.toByteArray();
        } catch (final SerialisationException e) {
            //re-throw SerialisationException
            throw e;
        } catch (final Exception e) {
            //wraps other exceptions.
            throw new SerialisationException(e.getMessage(), e);
        }
    }

    private ToBytesSerialiser nullCheck(final ToBytesSerialiser serialiser) throws SerialisationException {
        if (null == serialiser) {
            throw new SerialisationException(String.format("Serialiser for object type %s does not exist within the MultiSerialiser", Object.class));
        }
        return serialiser;
    }

    @Override
    public Object deserialise(final byte[] bytes) throws SerialisationException {
        try {
            byte keyByte = bytes[0];
            ToBytesSerialiser serialiser = nullCheck(supportedSerialisers.getSerialiserFromKey(keyByte));
            return serialiser.deserialise(bytes, 1, bytes.length - 1);
        } catch (final SerialisationException e) {
            //re-throw SerialisationException
            throw e;
        } catch (final Exception e) {
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
        return supportedSerialisers.canHandle(clazz);
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final MultiSerialiser serialiser = (MultiSerialiser) obj;

        return new EqualsBuilder()
                .append(supportedSerialisers, serialiser.supportedSerialisers)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(supportedSerialisers)
                .toHashCode();
    }
}
