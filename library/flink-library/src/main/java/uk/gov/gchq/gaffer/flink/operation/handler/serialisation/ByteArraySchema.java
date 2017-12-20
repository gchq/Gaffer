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
package uk.gov.gchq.gaffer.flink.operation.handler.serialisation;

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

import java.io.IOException;
import java.util.stream.Stream;

public class ByteArraySchema implements DeserializationSchema<Byte[]>, SerializationSchema<byte[]> {

    @Override
    public Byte[] deserialize(final byte[] bytes) throws IOException {
        return Stream.of(bytes).toArray(Byte[]::new);
    }

    @Override
    public boolean isEndOfStream(final Byte[] bytes) {
        return false;
    }

    @Override
    public BasicArrayTypeInfo<Byte[], Byte> getProducedType() {
        return BasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO;
    }

    @Override
    public byte[] serialize(final byte[] bytes) {
        return bytes;
    }
}
