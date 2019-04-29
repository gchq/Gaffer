/*
 * Copyright 2016-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.serialisation;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.exception.SerialisationException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

/**
 * An {@code AvroSerialiser} is used to serialise and deserialise Avro files.
 */
public class AvroSerialiser implements ToBytesSerialiser<Object> {

    private static final long serialVersionUID = -6264923181170362212L;
    private static final Logger LOGGER = LoggerFactory.getLogger(AvroSerialiser.class);

    @Override
    public byte[] serialise(final Object object) throws SerialisationException {
        Schema schema = ReflectData.get().getSchema(object.getClass());
        DatumWriter<Object> datumWriter = new ReflectDatumWriter<>(schema);
        DataFileWriter<Object> dataFileWriter = new DataFileWriter<>(datumWriter);
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        try {
            dataFileWriter.create(schema, byteOut);
            dataFileWriter.append(object);
            dataFileWriter.flush();
        } catch (final IOException e) {
            throw new SerialisationException("Unable to serialise given object of class: " + object.getClass().getName(), e);
        } finally {
            close(dataFileWriter);
        }
        return byteOut.toByteArray();
    }

    @Override
    public Object deserialise(final byte[] allBytes, final int offset, final int length) throws SerialisationException {
        final DatumReader<Object> datumReader = new ReflectDatumReader<>();
        try (final InputStream inputStream = new ByteArrayInputStream(allBytes, offset, length);
             final DataFileStream<Object> in = new DataFileStream<>(inputStream, datumReader)) {
            return in.next();
        } catch (final IOException e) {
            throw new SerialisationException("Unable to deserialise object, failed to read input bytes", e);
        }
    }

    @Override
    public Object deserialise(final byte[] bytes) throws SerialisationException {
        return deserialise(bytes, 0, bytes.length);
    }

    @Override
    public Object deserialiseEmpty() {
        return null;
    }

    @Override
    public boolean preservesObjectOrdering() {
        return false;
    }

    @Override
    public boolean canHandle(final Class clazz) {
        if ("java.lang.Class".equals(clazz.getName())) {
            return false;
        }
        try {
            ReflectData.get().getSchema(clazz);
        } catch (final RuntimeException e) {
            return false;
        }
        return true;
    }

    private void close(final Closeable close) {
        if (null != close) {
            try {
                close.close();
            } catch (final IOException e) {
                LOGGER.warn("Resource leak: unable to close stream in AvroSerialiser.class", e);
            }
        }
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
        return AvroSerialiser.class.getName().hashCode();
    }
}
