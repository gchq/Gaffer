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
package uk.gov.gchq.gaffer.flink.operation.handler;

import org.apache.flink.streaming.util.serialization.DeserializationSchema;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.flink.operation.handler.serialisation.ByteArraySchema;

import java.util.function.Function;

/**
 * Implementation of {@link GafferMapFunction} to allow Byte arrays representing {@link Element}s
 * to be mapped to Element objects.
 */
public class BytesMapFunction extends GafferMapFunction<Byte[]> {

    public BytesMapFunction() {

    }

    public BytesMapFunction(final Class<? extends Function<Iterable<? extends Byte[]>, Iterable<? extends Element>>> generatorClassName) {
        super(generatorClassName);
    }

    @Override
    public DeserializationSchema<Byte[]> getSerialisationType() {
        return new ByteArraySchema();
    }
}
