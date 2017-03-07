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
 * This class is used to serialise and deserialise avro files
 */
public class AvroSerialiser implements Serialisation<Object> {

    private static final long serialVersionUID = -6264923181170362212L;
    private static final Logger LOGGER = LoggerFactory.getLogger(AvroSerialiser.class);

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

    public Object deserialise(final byte[] bytes) throws SerialisationException {
        final DatumReader<Object> datumReader = new ReflectDatumReader<>();
        try (final InputStream inputStream = new ByteArrayInputStream(bytes);
             final DataFileStream<Object> in = new DataFileStream<>(inputStream, datumReader)) {
            return in.next();
        } catch (final IOException e) {
            throw new SerialisationException("Unable to deserialise object, failed to read input bytes", e);
        }
    }

    @Override
    public Object deserialiseEmptyBytes() {
        return null;
    }

    @Override
    public boolean preservesObjectOrdering() {
        return false;
    }

    public boolean canHandle(final Class clazz) {
        if (clazz.getName().equals("java.lang.Class")) {
            return false;
        }
        try {
            ReflectData.get().getSchema(clazz);
        } catch (RuntimeException e) {
            return false;
        }
        return true;
    }

    public <T> T deserialise(final byte[] bytes, final Class<T> clazz) throws SerialisationException {
        DatumReader<T> datumReader = new ReflectDatumReader<>();
        DataFileStream<T> in = null;
        T ret = null;
        try {
            in = new DataFileStream<>(new ByteArrayInputStream(bytes), datumReader);
            ret = in.next();
        } catch (final IOException e) {
            throw new SerialisationException("Unable to deserialise object, failed to read input bytes", e);
        } finally {
            close(in);
        }
        return ret;
    }

    private void close(final Closeable close) {
        if (close != null) {
            try {
                close.close();
            } catch (final IOException e) {
                LOGGER.warn("Resource leak: unable to close stream in AvroSerialiser.class", e);
            }
        }
    }
}
