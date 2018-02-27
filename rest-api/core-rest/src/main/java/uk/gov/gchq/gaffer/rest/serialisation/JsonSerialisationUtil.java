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
package uk.gov.gchq.gaffer.rest.serialisation;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.AnnotatedClass;
import com.fasterxml.jackson.databind.introspect.AnnotatedMethod;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import org.apache.commons.lang3.StringUtils;
import sun.reflect.generics.reflectiveObjects.TypeVariableImpl;

import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.koryphe.serialisation.json.SimpleClassNameIdResolver;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A utility class containing methods relevant to JSON Serialisation and Deserialisation,
 * for the REST API.
 */
public final class JsonSerialisationUtil {
    private static Map<String, Map<String, String>> cache = Collections.emptyMap();

    private JsonSerialisationUtil() {

    }

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
        final List<BeanPropertyDefinition> properties = introspection.findProperties();

        final Map<String, String> fieldMap = new HashMap<>();
        for (final BeanPropertyDefinition property : properties) {
            final String propName = property.getName();

            String propClass;
            if ("class".equals(propName)) {
                propClass = Class.class.getName();
            } else {
                Type genericType = null;
                if (null != property.getSetter() && null != property.getSetter().getGenericParameterTypes() && 1 == property.getSetter().getGenericParameterTypes().length) {
                    genericType = property.getSetter().getGenericParameterTypes()[0];
                }
                if (null == genericType) {
                    genericType = setterWithAnnotation(resolveMethods(property, "set"));
                }
                if (null == genericType) {
                    genericType = getterWithAnnotationAndNotIgnored(resolveMethods(property, "get"));
                }
                if (null == genericType && null != property.getGetter()) {
                    genericType = property.getGetter().getGenericReturnType();
                }
                if (null == genericType && null != property.getField()) {
                    genericType = property.getField().getGenericType();
                }
                if (null == genericType && null != property.getConstructorParameter()) {
                    genericType = property.getConstructorParameter().getParameterType();
                }
                if (null != genericType && genericType instanceof Class && ((Class) genericType).isEnum()) {
                    genericType = String.class;
                }
                if (null == genericType) {
                    propClass = Object.class.getName();
                } else {
                    if (genericType instanceof TypeVariableImpl
                            && null != ((TypeVariableImpl) genericType).getBounds()
                            && 1 == ((TypeVariableImpl) genericType).getBounds().length) {
                        propClass = ((TypeVariableImpl) genericType).getBounds()[0].getTypeName();
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

    /**
     * This method attempts to resolve the most specific and/or contextually relevant method for a given property,
     * in order to retrieve the parameter type for interacting with the property via JSON/REST.
     *
     * @param property the property for which the method should be resolved
     * @param getOrSet string to search for get or set methods
     * @return the parameter type for the method, or null
     */
    private static Iterable<AnnotatedMethod> resolveMethods(final BeanPropertyDefinition property, final String getOrSet) {
        if (null == property.getPrimaryMember()) {
            return new ArrayList<>();
        }

        final AnnotatedClass contextClass = property.getPrimaryMember().getContextClass();

        return Streams.toStream(contextClass.memberMethods())
                .filter(m -> StringUtils.containsIgnoreCase(m.getName(), property.getName()))
                .filter(m -> StringUtils.containsIgnoreCase(m.getName(), getOrSet))
                .filter(m -> m.getDeclaringClass().getName().equals(contextClass.getName()))
                .collect(Collectors.toList());
    }

    /**
     * Attempts to find a setter with a Json-related annotation, to avoid possible telescoping duplicates.
     *
     * @param methods the list of methods contextually related to a property
     * @return the parameter type for the setter
     */
    private static Type setterWithAnnotation(final Iterable<AnnotatedMethod> methods) {
        final List<Type> methodTypes = Streams.toStream(methods)
                .filter(m -> m.hasAnnotation(JsonProperty.class) || m.hasAnnotation(JsonSetter.class))
                .map(m -> m.getGenericParameterType(0))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        return methodTypes.isEmpty() ? setterWithoutAnnotation(methods) : methodTypes.get(0);
    }

    /**
     * If no annotated setter can be found, then attempt to use the most specific setter,
     * ie the setter from the context class.
     *
     * @param methods the list of methods contextually related to a property
     * @return the parameter type for the setter, or null
     */
    private static Type setterWithoutAnnotation(final Iterable<AnnotatedMethod> methods) {
        final List<Type> methodTypes = Streams.toStream(methods)
                .map(m -> m.getGenericParameterType(0))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        return methodTypes.isEmpty() ? null : methodTypes.get(0);
    }

    /**
     * Attempts to find a getter that is either designated as so by the JsonGetter annotation,
     * or at least a getter that is not marked with JsonIgnore.
     *
     * @param methods the list of methods contextually related to a property
     * @return the generic return type for the getter, or null
     */
    private static Type getterWithAnnotationAndNotIgnored(final Iterable<AnnotatedMethod> methods) {
        final List<Type> methodTypes = Streams.toStream(methods)
                .filter(m -> m.hasAnnotation(JsonGetter.class) || !m.hasAnnotation(JsonIgnore.class))
                .map(AnnotatedMethod::getGenericReturnType)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        return methodTypes.isEmpty() ? null : methodTypes.get(0);
    }

    private static boolean hasJsonIgnore(final Iterable<AnnotatedMethod> methods) {
        return !Streams.toStream(methods)
                .filter(m -> m.hasAnnotation(JsonIgnore.class))
                .collect(Collectors.toList())
                .isEmpty();
    }
}
