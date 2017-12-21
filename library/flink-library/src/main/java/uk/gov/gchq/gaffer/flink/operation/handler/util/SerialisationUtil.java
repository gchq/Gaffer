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
package uk.gov.gchq.gaffer.flink.operation.handler.util;

import org.apache.flink.streaming.util.serialization.DeserializationSchema;

import uk.gov.gchq.gaffer.flink.operation.handler.serialisation.ByteArraySchema;

import java.util.HashMap;
import java.util.Map;

public final class SerialisationUtil {

    private static final Map<Class<?>, DeserializationSchema> MAP = new HashMap<>();

    private static <T> void put(final Class<T> key, final DeserializationSchema<? extends T> value) {
        MAP.put(key, value);
    }

    private static <T> DeserializationSchema<T> get(final Class<T> subClass) {
        return (DeserializationSchema<T>) MAP.get(subClass);
    }

    static {
        put(Byte[].class, new ByteArraySchema());
    }

    public static class Resolver {
        public Resolver() { }

        public <T> TypeResolver<T> retrieve(final Class<T> clazz) {
            return new TypeResolver<>(clazz);
        }
    }

    public static final class TypeResolver<I> extends Resolver {
        private final Class<I> subType;

        private TypeResolver(final Class<I> clazz) {
            subType = clazz;
        }

        public DeserializationSchema<I> resolve() {
            return get(subType);
        }
    }
}
