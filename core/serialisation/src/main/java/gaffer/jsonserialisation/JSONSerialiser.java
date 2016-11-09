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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.util.ByteArrayBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
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

    private final ObjectMapper mapper;

    /**
     * Constructs a <code>JSONSerialiser</code> that skips nulls and default values.
     */
    public JSONSerialiser() {
        this(createDefaultMapper());
    }

    /**
     * Constructs a <code>JSONSerialiser</code> with a custom {@link ObjectMapper}.
     * To create the custom ObjectMapper it is advised that you start with the
     * default mapper provided from JSONSerialiser.createDefaultMapper() then
     * add your custom configuration.
     *
     * @param mapper a custom object mapper
     */
    public JSONSerialiser(final ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public static ObjectMapper createDefaultMapper() {
        final ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        mapper.configure(SerializationFeature.CLOSE_CLOSEABLE, true);
        return mapper;
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
        try (final InputStream stream2 = stream) {
            final byte[] bytes = IOUtils.readFully(stream2, stream.available(), true);
            return deserialise(bytes, clazz);
        } catch (IOException e) {
            throw new SerialisationException(e.getMessage(), e);
        }
    }

    /**
     * @param bytes the bytes of the object to deserialise
     * @param type  the type reference of the object to deserialise
     * @param <T>   the type of the object
     * @return the deserialised object
     * @throws SerialisationException if the bytes fail to deserialise
     */
    public <T> T deserialise(final byte[] bytes, final TypeReference<T> type) throws SerialisationException {
        try {
            return mapper.readValue(bytes, type);
        } catch (IOException e) {
            throw new SerialisationException(e.getMessage(), e);
        }
    }

    /**
     * @param stream the {@link java.io.InputStream} containing the bytes of the object to deserialise
     * @param type   the type reference of the object to deserialise
     * @param <T>    the type of the object
     * @return the deserialised object
     * @throws SerialisationException if the bytes fail to deserialise
     */
    public <T> T deserialise(final InputStream stream, final TypeReference<T> type) throws SerialisationException {
        try (final InputStream stream2 = stream) {
            final byte[] bytes = IOUtils.readFully(stream2, stream.available(), true);
            return deserialise(bytes, type);
        } catch (IOException e) {
            throw new SerialisationException(e.getMessage(), e);
        }
    }

    public ObjectMapper getMapper() {
        return mapper;
    }
}
