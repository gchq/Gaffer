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
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import uk.gov.gchq.gaffer.flink.operation.handler.serialisation.ByteArraySchema;

import java.util.HashMap;
import java.util.Map;

public final class SerialisationUtil {
    private static final Map<String, Class<?>> STRING_TO_CLASS = new HashMap<>();
    private static final Map<Class, DeserializationSchema> DESERIALIZATION_SCHEMAS = new HashMap<>();

    private SerialisationUtil() {
        // util class
    }

    static {
        DESERIALIZATION_SCHEMAS.put(String.class, new SimpleStringSchema());
        DESERIALIZATION_SCHEMAS.put(byte[].class, new ByteArraySchema());


        STRING_TO_CLASS.put("java.lang.String", String.class);
        STRING_TO_CLASS.put("[B", byte[].class);
    }

//    public static <T> DeserializationSchema<T> getFromMap(final String type) throws IllegalAccessException, InstantiationException {
//        return test(STRING_TO_CLASS.get(type));
//    }
//
//    private static <R> DeserializationSchema<R> test(final Class<R> clazz) {
//        return DESERIALIZATION_SCHEMAS.get(clazz);
//    }

    public static class Resolver {
        public Resolver() {
            new TypeResolver<>(String.class);
        }

        public <T> Resolver(final Class<T> clazz) {
            new TypeResolver<>(clazz);
        }
    }

    public static final class TypeResolver<I> extends Resolver {
        private final Class<I> subType;

        private TypeResolver(final Class<I> clazz) {
            subType = clazz;
        }

        // TODO retrieve appropriate Schema for the subType, return Schema

        public DeserializationSchema<I> resolve() {
            return null;
        }

    }
}
