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

package gaffer.data.elementdefinition.view;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import gaffer.data.element.ElementComponentKey;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.element.function.ElementTransformer;
import gaffer.data.elementdefinition.ElementDefinition;
import gaffer.data.elementdefinition.exception.SchemaException;
import gaffer.function.FilterFunction;
import gaffer.function.TransformFunction;
import gaffer.function.context.ConsumerFunctionContext;
import gaffer.function.context.ConsumerProducerFunctionContext;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * A <code>ViewElementDefinition</code> is an {@link ElementDefinition} containing
 * transient properties, an {@link ElementTransformer} and a {@link ElementFilter}.
 */
public class ViewElementDefinition implements ElementDefinition {
    private ElementTransformer transformer;
    private ElementFilter filter;

    /**
     * Transient property map of property name to class.
     */
    private LinkedHashMap<String, Class<?>> transientProperties = new LinkedHashMap<>();

    @Override
    public void merge(final ElementDefinition elementDef) {
        if (elementDef instanceof ViewElementDefinition) {
            merge(((ViewElementDefinition) elementDef));
        } else {
            throw new IllegalArgumentException("Cannot merge a schema element definition with a " + elementDef.getClass());
        }
    }

    public void merge(final ViewElementDefinition elementDef) {
        for (Entry<String, Class<?>> entry : elementDef.getTransientPropertyMap().entrySet()) {
            final String newProp = entry.getKey();
            final Class<?> newPropClass = entry.getValue();
            if (!transientProperties.containsKey(newProp)) {
                transientProperties.put(newProp, newPropClass);
            } else {
                final Class<?> clazz = transientProperties.get(newProp);
                if (!clazz.equals(newPropClass)) {
                    throw new SchemaException("Unable to merge schemas. Conflict of transient property classes for " + newProp
                            + ". Classes are: " + clazz.getName() + " and " + newPropClass.getName());
                }
            }
        }
    }

    public Class<?> getTransientPropertyClass(final String propertyName) {
        return transientProperties.get(propertyName);
    }

    @JsonIgnore
    public Collection<Class<?>> getTransientPropertyClasses() {
        return transientProperties.values();
    }

    public Set<String> getTransientProperties() {
        return transientProperties.keySet();
    }

    public boolean containsTransientProperty(final String propertyName) {
        return transientProperties.containsKey(propertyName);
    }

    /**
     * @return the transient property map. {@link LinkedHashMap} of transient property name to class name.
     */
    @JsonIgnore
    public Map<String, Class<?>> getTransientPropertyMap() {
        return Collections.unmodifiableMap(transientProperties);
    }

    @JsonGetter("transientProperties")
    public Map<String, String> getTransientPropertyMapWithClassNames() {
        Map<String, String> propertyMap = new HashMap<>();
        for (Entry<String, Class<?>> entry : transientProperties.entrySet()) {
            propertyMap.put(entry.getKey(), entry.getValue().getName());
        }

        return propertyMap;
    }

    /**
     * Set the transient properties.
     *
     * @param newTransientProperties {@link LinkedHashMap} of transient property name to class name.
     * @throws ClassNotFoundException thrown if any of the property class names could not be found.
     */
    @JsonSetter("transientProperties")
    public void setTransientPropertyMapWithClassNames(final LinkedHashMap<String, String> newTransientProperties) throws ClassNotFoundException {
        transientProperties = new LinkedHashMap<>();
        for (Entry<String, String> entry : newTransientProperties.entrySet()) {
            transientProperties.put(entry.getKey(), Class.forName(entry.getValue()));
        }
    }

    @JsonIgnore
    public ElementFilter getFilter() {
        return filter;
    }

    public void setFilter(final ElementFilter filter) {
        this.filter = filter;
    }

    @JsonGetter("filterFunctions")
    public List<ConsumerFunctionContext<ElementComponentKey, FilterFunction>> getFilterFunctions() {
        return null != filter ? filter.getFunctions() : null;
    }

    @JsonSetter("filterFunctions")
    public void addFilterFunctions(final List<ConsumerFunctionContext<ElementComponentKey, FilterFunction>> functions) {
        if (null == filter) {
            filter = new ElementFilter();
        }

        filter.addFunctions(functions);
    }

    @JsonIgnore
    public ElementTransformer getTransformer() {
        return transformer;
    }

    public void setTransformer(final ElementTransformer transformer) {
        this.transformer = transformer;
    }

    @JsonGetter("transformFunctions")
    public List<ConsumerProducerFunctionContext<ElementComponentKey, TransformFunction>> getTransformFunctions() {
        return null != transformer ? transformer.getFunctions() : null;
    }

    @JsonSetter("transformFunctions")
    public void addTransformFunctions(final List<ConsumerProducerFunctionContext<ElementComponentKey, TransformFunction>> functions) {
        transformer = new ElementTransformer();
        transformer.addFunctions(functions);
    }

    public static class Builder {
        private final ViewElementDefinition elDef;

        public Builder() {
            this.elDef = new ViewElementDefinition();
        }

        public Builder transientProperty(final String propertyName, final Class<?> clazz) {
            elDef.transientProperties.put(propertyName, clazz);
            return this;
        }

        public Builder filter(final ElementFilter filter) {
            getElementDef().setFilter(filter);
            return this;
        }

        public Builder transformer(final ElementTransformer transformer) {
            getElementDef().setTransformer(transformer);
            return this;
        }

        public ViewElementDefinition build() {
            return elDef;
        }

        public ViewElementDefinition getElementDef() {
            return elDef;
        }
    }
}
