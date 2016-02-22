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

package gaffer.jsonserialisation;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.core.util.ByteArrayBuilder;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import gaffer.exception.SerialisationException;
import sun.misc.IOUtils;

import java.io.IOException;
import java.io.InputStream;

/**
 * A <code>JSONSerialiser</code> provides the ability to serialise and deserialise to/from JSON.
 * The serialisation is set to not include nulls or default values.
 */
public class JSONSerialiser {
    private static final JsonFactory JSON_FACTORY = new JsonFactory();

    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * Constructs a <code>JSONSerialiser</code> that skips nulls and default values.
     */
    public JSONSerialiser() {
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
    }

    /**
     * Constructs a <code>JSONSerialiser</code> that skips nulls and default values and adds the custom serialisers.
     *
     * @param customTypeSerialisers custom type {@link com.fasterxml.jackson.databind.JsonSerializer}
     */
    public JSONSerialiser(final JsonSerializer... customTypeSerialisers) {
        this();
        final SimpleModule module = new SimpleModule("custom", new Version(1, 0, 0, null, null, null));
        for (JsonSerializer customTypeSerialiser : customTypeSerialisers) {
            module.addSerializer(customTypeSerialiser);
        }
        mapper.registerModule(module);
    }

    /**
     * @param clazz the clazz of the object to be serialised/deserialised
     * @return true if the clazz can be serialised/deserialised
     */
    public boolean canHandle(final Class clazz) {
        return mapper.canSerialize(clazz);
    }

    /**
     * Serialises an object.
     *
     * @param object the object to be serialised
     * @return the provided object serialised into bytes
     * @throws SerialisationException if the object fails to be serialised
     */
    public byte[] serialise(final Object object) throws SerialisationException {
        return serialise(object, false);
    }

    /**
     * Serialises an object.
     *
     * @param object      the object to be serialised
     * @param prettyPrint true if the object should be serialised with pretty printing
     * @return the provided object serialised (with pretty printing) into bytes
     * @throws SerialisationException if the object fails to serialise
     */
    public byte[] serialise(final Object object, final boolean prettyPrint) throws SerialisationException {
        final ByteArrayBuilder byteArrayBuilder = new ByteArrayBuilder();
        try {
            serialise(object, JSON_FACTORY.createGenerator(byteArrayBuilder, JsonEncoding.UTF8), prettyPrint);
        } catch (IOException e) {
            throw new SerialisationException(e.getMessage(), e);
        }

        return byteArrayBuilder.toByteArray();
    }

    /**
     * Serialises an object using the provided json generator.
     *
     * @param object        the object to be serialised and written to file
     * @param jsonGenerator the {@link com.fasterxml.jackson.core.JsonGenerator} to use to write the object to
     * @param prettyPrint   true if the object should be serialised with pretty printing
     * @throws SerialisationException if the object fails to serialise
     */
    public void serialise(final Object object, final JsonGenerator jsonGenerator, final boolean prettyPrint)
            throws SerialisationException {
        if (prettyPrint) {
            jsonGenerator.useDefaultPrettyPrinter();
        }

        try {
            mapper.writeValue(jsonGenerator, object);
        } catch (IOException e) {
            throw new SerialisationException("Failed to serialise object to json: " + e.getMessage(), e);
        }
    }

    /**
     * @param bytes the bytes of the object to deserialise
     * @param clazz the class of the object to deserialise
     * @param <T>   the type of the object
     * @return the deserialised object
     * @throws SerialisationException if the bytes fail to deserialise
     */
    public <T> T deserialise(final byte[] bytes, final Class<T> clazz) throws SerialisationException {
        try {
            return mapper.readValue(bytes, clazz);
        } catch (IOException e) {
            throw new SerialisationException(e.getMessage(), e);
        }
    }

    /**
     * @param stream the {@link java.io.InputStream} containing the bytes of the object to deserialise
     * @param clazz  the class of the object to deserialise
     * @param <T>    the type of the object
     * @return the deserialised object
     * @throws SerialisationException if the bytes fail to deserialise
     */
    public <T> T deserialise(final InputStream stream, final Class<T> clazz) throws SerialisationException {
        try {
            final byte[] bytes = IOUtils.readFully(stream, stream.available(), true);
            return deserialise(bytes, clazz);
        } catch (IOException e) {
            throw new SerialisationException(e.getMessage(), e);
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException("Unable to close stream : " + e.getMessage(), e);
            }
        }
    }
}
