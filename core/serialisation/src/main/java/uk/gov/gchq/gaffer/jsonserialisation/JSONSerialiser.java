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

package uk.gov.gchq.gaffer.jsonserialisation;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.util.ByteArrayBuilder;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.BeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.jackson.CloseableIterableDeserializer;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * A <code>JSONSerialiser</code> provides the ability to serialise and deserialise to/from JSON.
 * The serialisation is set to not include nulls or default values.
 */
public class JSONSerialiser {
    public static final String FILTER_FIELDS_BY_NAME = "filterFieldsByName";
    private static final JsonFactory JSON_FACTORY = new JsonFactory();

    private final ObjectMapper mapper;
    private final List<SerialiserComponent> customSerialisers = new ArrayList<>();

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

    public List<SerialiserComponent> getSerialisers() {
        return customSerialisers;
    }

    public void setSerialisers(final List<SerialiserComponent> serialisers) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        customSerialisers.addAll(serialisers);
        final SimpleModule module = new SimpleModule("CustomSerialisers", new Version(1, 0, 0, null));
        for (final SerialiserComponent serialiser : serialisers) {
            if (null != serialiser.serialiserClass) {
                module.addSerializer(Class.forName(serialiser.objectClass), Class.forName(serialiser.serialiserClass).asSubclass(JsonSerializer.class).newInstance());
            }
            if (null != serialiser.deserialiserClass) {
                module.addDeserializer(Class.forName(serialiser.objectClass), Class.forName(serialiser.deserialiserClass).asSubclass(JsonDeserializer.class).newInstance());
            }
        }
        mapper.registerModule(module);
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
        } catch (IOException e) {
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
            final byte[] bytes = IOUtils.toByteArray(stream2);
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
            final byte[] bytes = IOUtils.toByteArray(stream2);
            return deserialise(bytes, type);
        } catch (IOException e) {
            throw new SerialisationException(e.getMessage(), e);
        }
    }

    @JsonIgnore
    public ObjectMapper getMapper() {
        return mapper;
    }

    public static class SerialiserComponent {
        public String objectClass;
        public String serialiserClass;
        public String deserialiserClass;

        public SerialiserComponent() {
        }

        public SerialiserComponent(final Class<?> objectClass, final Class<?> serialiserClass, final Class<?> deserialiserClass) {
            this.objectClass = objectClass.getName();
            this.serialiserClass = serialiserClass.getName();
            this.deserialiserClass = deserialiserClass.getName();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(objectClass)
                    .append(serialiserClass)
                    .append(deserialiserClass)
                    .toHashCode();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final SerialiserComponent that = (SerialiserComponent) o;
            return new EqualsBuilder()
                    .append(objectClass, that.objectClass)
                    .append(serialiserClass, that.serialiserClass)
                    .append(deserialiserClass, that.deserialiserClass)
                    .isEquals();
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("objectClass", objectClass)
                    .append("serialiserClass", serialiserClass)
                    .append("deserialiserClass", deserialiserClass)
                    .toString();
        }
    }
}
