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

import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.AnnotatedClass;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;

import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.koryphe.serialisation.json.SimpleClassNameIdResolver;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class JsonSerialisationUtil {

    public static Map<String, String> getSerialisedFieldClasses(final String className) {
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
            Type genericType = null;

            if ("class".equals(propName)) {
                genericType = clazz;
            }

            if (null != property.getSetter() && null == genericType) {
                genericType = resolveConcreteSetterParameterType(property);
            }

            // TODO if genType is null, try property setter? Similar to above but without comparison to super class?

            if (null != property.getGetter() && null == genericType) {
                genericType = property.getGetter().getGenericReturnType();
            }

            if (null != property.getField() && null == genericType) {
                System.out.println(property.getField());
                genericType = property.getField().getGenericType();
            }

            // TODO check if parameter type is generic, if so return Object.class

            if (genericType instanceof Class && ((Class) genericType).isEnum()) {
                genericType = String.class;
            }

            fieldMap.put(propName, genericType != null ? genericType.getTypeName() : Object.class.getName());
        }

        return fieldMap;
    }

    /**
     * This method attempts to resolve the parameter type of a setter from the context of the property.
     * Initially designed for {@link #getSerialisedFieldClasses(String)}, whereby if an Interface method was
     * annotated with JsonPropertyInfo, the field class was resolved to a generic type.
     * This now provides accurate type info for passing values via REST (since the setter information is used)
     *
     * @param property the property for which the type should be resolved
     * @return the type for a given field, to be passed via REST
     */
    private static Type resolveConcreteSetterParameterType(final BeanPropertyDefinition property) {
        final AnnotatedClass contextClass = property.getPrimaryMember().getContextClass();

        final List<Type> methodTypes = Streams.toStream(contextClass.memberMethods())
                .filter(m -> m.getName().equals(property.getSetter().getName()))
                .filter(m -> m.getDeclaringClass().getName().equals(contextClass.getName()))
                .map(m -> m.getGenericParameterType(0))
                .collect(Collectors.toList());

        // TODO modify so that if there are multiple setters, only the parameter type of the annotated method is used (see @ IsIn#setAllowedValues)
        // maybe check for an annotation on the setter during the stream, filter by that?
        // or leave as AnnotatedMethods in the stream, and if size >1, check for JsonTypeInfo annotation?
        return methodTypes.isEmpty() ? null : methodTypes.get(0);
    }
}
