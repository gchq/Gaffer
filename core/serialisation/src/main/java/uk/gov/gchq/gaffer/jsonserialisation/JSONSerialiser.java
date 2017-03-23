/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.jsonserialisation;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.util.ByteArrayBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.BeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import org.apache.commons.io.IOUtils;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.jackson.CloseableIterableDeserializer;
import java.io.IOException;
import java.io.InputStream;

/**
 * A <code>JSONSerialiser</code> provides the ability to serialise and deserialise to/from JSON.
 * The serialisation is set to not include nulls or default values.
 */
public class JSONSerialiser {
    public static final String FILTER_FIELDS_BY_NAME = "filterFieldsByName";
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

    public static JSONSerialiser fromClass(final String className) {
        if (null == className || className.isEmpty()) {
            return new JSONSerialiser();
        }

        try {
            return fromClass(Class.forName(className).asSubclass(JSONSerialiser.class));
        } catch (final ClassNotFoundException e) {
            throw new IllegalArgumentException("Could not create instance of json serialiser from class: " + className);
        }
    }

    public static JSONSerialiser fromClass(final Class<? extends JSONSerialiser> clazz) {
        if (null == clazz) {
            return new JSONSerialiser();
        }

        try {
            return clazz.newInstance();
        } catch (final InstantiationException | IllegalAccessException e) {
            throw new IllegalArgumentException("Could not create instance of json serialiser from class: " + clazz.getName());
        }
    }

    public static ObjectMapper createDefaultMapper() {
        final ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        mapper.configure(SerializationFeature.CLOSE_CLOSEABLE, true);
        mapper.registerModule(getCloseableIterableDeserialiserModule());

        // Use the 'setFilters' method so it is compatible with older versions of jackson
        mapper.setFilters(getFilterProvider());
        return mapper;
    }

    private static SimpleModule getCloseableIterableDeserialiserModule() {
        SimpleModule module = new SimpleModule();
        module.addDeserializer(CloseableIterable.class, new CloseableIterableDeserializer());
        return module;
    }

    public static FilterProvider getFilterProvider(final String... fieldsToExclude) {
        if (null == fieldsToExclude || fieldsToExclude.length == 0) {
            // Use the 'serializeAllExcept' method so it is compatible with older versions of jackson
            return new SimpleFilterProvider()
                    .addFilter(FILTER_FIELDS_BY_NAME, (BeanPropertyFilter) SimpleBeanPropertyFilter.serializeAllExcept());
        }

        return new SimpleFilterProvider()
                .addFilter(FILTER_FIELDS_BY_NAME, (BeanPropertyFilter) SimpleBeanPropertyFilter.serializeAllExcept(fieldsToExclude));
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
     * @param object          the object to be serialised
     * @param fieldsToExclude optional property names to exclude from the json
     * @return the provided object serialised into bytes
     * @throws SerialisationException if the object fails to be serialised
     */
    public byte[] serialise(final Object object, final String... fieldsToExclude) throws SerialisationException {
        return serialise(object, false, fieldsToExclude);
    }


    /**
     * Serialises an object.
     *
     * @param object          the object to be serialised
     * @param prettyPrint     true if the object should be serialised with pretty printing
     * @param fieldsToExclude optional property names to exclude from the json
     * @return the provided object serialised (with pretty printing) into bytes
     * @throws SerialisationException if the object fails to serialise
     */
    public byte[] serialise(final Object object, final boolean prettyPrint, final String... fieldsToExclude) throws SerialisationException {
        final ByteArrayBuilder byteArrayBuilder = new ByteArrayBuilder();
        try {
            serialise(object, JSON_FACTORY.createGenerator(byteArrayBuilder, JsonEncoding.UTF8), prettyPrint, fieldsToExclude);
        } catch (final IOException e) {
            throw new SerialisationException(e.getMessage(), e);
        }

        return byteArrayBuilder.toByteArray();
    }

    /**
     * Serialises an object using the provided json generator.
     *
     * @param object          the object to be serialised and written to file
     * @param jsonGenerator   the {@link com.fasterxml.jackson.core.JsonGenerator} to use to write the object to
     * @param prettyPrint     true if the object should be serialised with pretty printing
     * @param fieldsToExclude optional property names to exclude from the json
     * @throws SerialisationException if the object fails to serialise
     */
    public void serialise(final Object object, final JsonGenerator jsonGenerator, final boolean prettyPrint, final String... fieldsToExclude)
            throws SerialisationException {
        if (prettyPrint) {
            jsonGenerator.useDefaultPrettyPrinter();
        }

        final ObjectWriter writer = mapper.writer(getFilterProvider(fieldsToExclude));
        try {
            writer.writeValue(jsonGenerator, object);
        } catch (final IOException e) {
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
        } catch (final IOException e) {
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
            final byte[] bytes = IOUtils.toByteArray(stream2);
            return deserialise(bytes, clazz);
        } catch (final IOException e) {
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
        } catch (final IOException e) {
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
            final byte[] bytes = IOUtils.toByteArray(stream2);
            return deserialise(bytes, type);
        } catch (final IOException e) {
            throw new SerialisationException(e.getMessage(), e);
        }
    }

    @JsonIgnore
    public ObjectMapper getMapper() {
        return mapper;
    }
}
