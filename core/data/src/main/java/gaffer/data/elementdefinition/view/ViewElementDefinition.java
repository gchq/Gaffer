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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * A <code>ViewElementDefinition</code> is an {@link ElementDefinition} containing
 * transient properties, an {@link ElementTransformer} and two {@link ElementFilter}'s.
 */
public class ViewElementDefinition implements ElementDefinition {
    private ElementTransformer transformer;
    private ElementFilter preAggregationFilter;
    private ElementFilter postAggregationFilter;
    private ElementFilter postTransformFilter;


    /**
     * This field overrides the group by properties in the schema.
     * They must be sub set of the group by properties in the schema.
     * If the store is ordered, then it must be a truncated copy of the schema
     * group by properties.
     * <p>
     * If null, then the group by properties in the schema are used.
     * </p>
     * <p>
     * If empty, then all group by properties are summarised.
     * </p>
     * <p>
     * If 1 or more properties, then the specified properties are not
     * summarised.
     * </p>
     */
    private LinkedHashSet<String> groupBy;

    /**
     * Transient property map of property name to class.
     */
    private LinkedHashMap<String, Class<?>> transientProperties = new LinkedHashMap<>();

    public LinkedHashSet<String> getGroupBy() {
        return groupBy;
    }

    public void setGroupBy(final LinkedHashSet<String> groupBy) {
        this.groupBy = groupBy;
    }

    @Override
    public void merge(final ElementDefinition elementDef) {
        if (elementDef instanceof ViewElementDefinition) {
            merge(((ViewElementDefinition) elementDef));
        } else {
            throw new IllegalArgumentException("Cannot merge a schema element definition with a " + elementDef.getClass());
        }
    }

    public void merge(final ViewElementDefinition elementDef) {
        for (final Entry<String, Class<?>> entry : elementDef.getTransientPropertyMap().entrySet()) {
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
        for (final Entry<String, Class<?>> entry : transientProperties.entrySet()) {
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
        for (final Entry<String, String> entry : newTransientProperties.entrySet()) {
            transientProperties.put(entry.getKey(), Class.forName(entry.getValue()));
        }
    }

    @JsonIgnore
    public ElementFilter getPreAggregationFilter() {
        return preAggregationFilter;
    }

    public void setPreAggregationFilter(final ElementFilter preAggregationFilter) {
        this.preAggregationFilter = preAggregationFilter;
    }

    @JsonGetter("preAggregationFilterFunctions")
    public List<ConsumerFunctionContext<String, FilterFunction>> getPreAggregationFilterFunctions() {
        return null != preAggregationFilter ? preAggregationFilter.getFunctions() : null;
    }

    @JsonSetter("preAggregationFilterFunctions")
    public void addPreAggregationElementFilterFunctions(final List<ConsumerFunctionContext<String, FilterFunction>> functions) {
        if (null == preAggregationFilter) {
            preAggregationFilter = new ElementFilter();
        }

        preAggregationFilter.addFunctions(functions);
    }

    @JsonIgnore
    public ElementFilter getPostAggregationFilter() {
        return postAggregationFilter;
    }

    public void setPostAggregationFilter(final ElementFilter postAggregationFilter) {
        this.postAggregationFilter = postAggregationFilter;
    }

    @JsonGetter("postAggregationFilterFunctions")
    public List<ConsumerFunctionContext<String, FilterFunction>> getPostAggregationFilterFunctions() {
        return null != postAggregationFilter ? postAggregationFilter.getFunctions() : null;
    }

    @JsonSetter("postAggregationFilterFunctions")
    public void addPostAggregationElementFilterFunctions(final List<ConsumerFunctionContext<String, FilterFunction>> functions) {
        if (null == postAggregationFilter) {
            postAggregationFilter = new ElementFilter();
        }

        postAggregationFilter.addFunctions(functions);
    }

    @JsonIgnore
    public ElementFilter getPostTransformFilter() {
        return postTransformFilter;
    }

    public void setPostTransformFilter(final ElementFilter postFilter) {
        this.postTransformFilter = postFilter;
    }

    @JsonGetter("postTransformFilterFunctions")
    public List<ConsumerFunctionContext<String, FilterFunction>> getPostTransformFilterFunctions() {
        return null != postTransformFilter ? postTransformFilter.getFunctions() : null;
    }

    @JsonSetter("postTransformFilterFunctions")
    public void addPostTransformFilterFunctions(final List<ConsumerFunctionContext<String, FilterFunction>> functions) {
        if (null == postTransformFilter) {
            postTransformFilter = new ElementFilter();
        }

        postTransformFilter.addFunctions(functions);
    }

    @JsonIgnore
    public ElementTransformer getTransformer() {
        return transformer;
    }

    public void setTransformer(final ElementTransformer transformer) {
        this.transformer = transformer;
    }

    @JsonGetter("transformFunctions")
    public List<ConsumerProducerFunctionContext<String, TransformFunction>> getTransformFunctions() {
        return null != transformer ? transformer.getFunctions() : null;
    }

    @JsonSetter("transformFunctions")
    public void addTransformFunctions(final List<ConsumerProducerFunctionContext<String, TransformFunction>> functions) {
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

        public Builder preAggregationFilter(final ElementFilter filter) {
            getElementDef().setPreAggregationFilter(filter);
            return this;
        }

        public Builder postAggregationFilter(final ElementFilter filter) {
            getElementDef().setPostAggregationFilter(filter);
            return this;
        }

        public Builder postTransformFilter(final ElementFilter postFilter) {
            getElementDef().setPostTransformFilter(postFilter);
            return this;
        }

        public Builder transformer(final ElementTransformer transformer) {
            getElementDef().setTransformer(transformer);
            return this;
        }

        public Builder groupBy(final String... groupBy) {
            if (null == getElementDef().getGroupBy()) {
                getElementDef().setGroupBy(new LinkedHashSet<String>());
            }
            Collections.addAll(getElementDef().getGroupBy(), groupBy);
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
