/*
 * Copyright 2017-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.serialisation.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;

import uk.gov.gchq.koryphe.serialisation.json.SimpleClassNameIdResolver;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A utility class containing methods relevant to JSON Serialisation and Deserialisation.
 */
public final class JsonSerialisationUtil {
    private static Map<String, Map<String, String>> cache = Collections.emptyMap();

    private JsonSerialisationUtil() {

    }

    /**
     * Gets all the fields and their classes for a given class.
     *
     * @param className the class name to find the fields for.
     * @return a map of field name to class name
     */
    public static Map<String, String> getSerialisedFieldClasses(final String className) {
        final Map<String, String> cachedResult = cache.get(className);
        if (null != cachedResult) {
            return cachedResult;
        }

        final Class<?> clazz;
        try {
            clazz = Class.forName(SimpleClassNameIdResolver.getClassName(className));
        } catch (final Exception e) {
            throw new IllegalArgumentException("Class name was not recognised: " + className, e);
        }

        final ObjectMapper mapper = new ObjectMapper();
        final JavaType type = mapper.getTypeFactory().constructType(clazz);
        final BeanDescription introspection = mapper.getSerializationConfig()
                .introspect(type);

        final Class<?> builder = introspection.findPOJOBuilder();
        String buildMethodPrefix = "with";
        if (null != builder) {
            JsonPOJOBuilder anno = findAnnotation(builder, JsonPOJOBuilder.class);
            if (null != anno) {
                buildMethodPrefix = anno.withPrefix();
            }
        }

        Constructor<?> creator = null;
        for (final Constructor<?> constructor : type.getRawClass().getDeclaredConstructors()) {
            final JsonCreator anno = constructor.getAnnotation(JsonCreator.class);
            if (null != anno) {
                creator = constructor;
                break;
            }
        }

        final List<BeanPropertyDefinition> properties = introspection.findProperties();

        final Map<String, String> fieldMap = new HashMap<>();
        for (final BeanPropertyDefinition property : properties) {
            final String propName = property.getName();

            String propClass;
            if ("class".equals(propName)) {
                propClass = Class.class.getName();
            } else {
                Type genericType = null;
                if (null != builder) {
                    final String methodName = buildMethodPrefix + propName;
                    for (final Method method : builder.getMethods()) {
                        if (methodName.equalsIgnoreCase(method.getName())) {
                            final Type[] params = method.getGenericParameterTypes();
                            if (null != params && 1 == params.length) {
                                genericType = params[0];
                            }
                            break;
                        }
                    }
                }
                if (null == genericType && null != creator) {
                    for (final Parameter parameter : creator.getParameters()) {
                        final JsonProperty anno = parameter.getAnnotation(JsonProperty.class);
                        if (null != anno && propName.equals(anno.value())) {
                            if (null != parameter.getParameterizedType()) {
                                genericType = parameter.getParameterizedType();
                            } else {
                                genericType = parameter.getType();
                            }
                            break;
                        }
                    }
                }
                if (null == genericType && null != property.getSetter() && null != property.getSetter().getGenericParameterTypes() && 1 == property.getSetter().getGenericParameterTypes().length) {
                    genericType = property.getSetter().getGenericParameterTypes()[0];
                }
                if (null != genericType && genericType instanceof Class && ((Class) genericType).isEnum()) {
                    genericType = String.class;
                }
                if (null == genericType) {
                    propClass = Object.class.getName();
                } else {
                    if (genericType instanceof TypeVariable
                            && null != ((TypeVariable) genericType).getBounds()
                            && 1 == ((TypeVariable) genericType).getBounds().length) {
                        propClass = ((ParameterizedType) ((TypeVariable) genericType).getBounds()[0]).getRawType().getTypeName();
                    } else {
                        propClass = genericType.getTypeName();
                    }
                    if (null != propClass) {
                        // Try and replace any primitive types with the full class name, e.g int/boolean with java.lang.Integer/java.lang.Boolean
                        if (!propClass.contains(".")) {
                            propClass = SimpleClassNameIdResolver.getClassName(propClass);
                        }
                        propClass = propClass.replaceAll("\\? extends ", "")
                                .replaceAll("\\? super ", "")
                                .replaceAll(" ", "");
                    }
                }
            }

            fieldMap.put(propName, propClass);
        }

        final Map<String, Map<String, String>> newCache = new HashMap<>(cache);
        newCache.put(className, Collections.unmodifiableMap(fieldMap));
        cache = Collections.unmodifiableMap(newCache);

        return fieldMap;
    }

    private static <T extends Annotation> T findAnnotation(final Class<?> builderclass, final Class<T> annotationClass) {
        T anno = builderclass.getAnnotation(annotationClass);
        if (null == anno) {
            Class<?> superClass = builderclass.getSuperclass();
            while (null != superClass && null == anno) {
                anno = superClass.getAnnotation(annotationClass);
                if (null == anno) {
                    superClass = superClass.getSuperclass();
                }
            }
        }
        if (null == anno) {
            for (final Class<?> interfaceClass : builderclass.getInterfaces()) {
                if (null != interfaceClass) {
                    anno = interfaceClass.getAnnotation(annotationClass);
                    if (null != anno) {
                        break;
                    }
                }
            }
        }
        return anno;
    }
}
