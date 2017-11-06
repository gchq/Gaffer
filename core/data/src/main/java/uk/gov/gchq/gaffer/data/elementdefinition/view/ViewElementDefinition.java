/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.data.elementdefinition.view;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.collect.Sets;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.elementdefinition.ElementDefinition;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.tuple.function.TupleAdaptedFunction;
import uk.gov.gchq.koryphe.tuple.predicate.TupleAdaptedPredicate;

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
 * A {@code ViewElementDefinition} is an {@link ElementDefinition} containing
 * transient properties, an {@link ElementTransformer} and two {@link ElementFilter}s.
 */
@JsonDeserialize(builder = ViewElementDefinition.Builder.class)
public class ViewElementDefinition implements ElementDefinition {
    protected ElementFilter preAggregationFilter;
    protected ElementFilter postAggregationFilter;
    protected ElementAggregator aggregator;
    protected ElementFilter postTransformFilter;
    protected ElementTransformer transformer;

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
    protected Set<String> groupBy;

    protected Set<String> properties;

    protected Set<String> excludeProperties;

    /**
     * Transient property map of property name to class.
     */
    protected Map<String, Class<?>> transientProperties = new LinkedHashMap<>();

    public Set<String> getGroupBy() {
        return groupBy;
    }

    public void setGroupBy(final LinkedHashSet<String> groupBy) {
        this.groupBy = groupBy;
    }

    public Set<String> getProperties() {
        return properties;
    }

    public Set<String> getExcludeProperties() {
        return excludeProperties;
    }

    @JsonIgnore
    public boolean isAllProperties() {
        return null == properties && (null == excludeProperties || excludeProperties.isEmpty());
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
        return transientProperties;
    }

    @JsonGetter("transientProperties")
    public Map<String, String> getTransientPropertyMapWithClassNames() {
        if (transientProperties.isEmpty()) {
            return null;
        }

        Map<String, String> propertyMap = new HashMap<>();
        for (final Entry<String, Class<?>> entry : transientProperties.entrySet()) {
            propertyMap.put(entry.getKey(), entry.getValue().getName());
        }

        return propertyMap;
    }

    @JsonIgnore
    public ElementFilter getPreAggregationFilter() {
        return preAggregationFilter;
    }

    @JsonGetter("preAggregationFilterFunctions")
    public List<TupleAdaptedPredicate<String, ?>> getPreAggregationFilterFunctions() {
        return null != preAggregationFilter ? preAggregationFilter.getComponents() : null;
    }

    public boolean hasPreAggregationFilters() {
        return null != preAggregationFilter && !preAggregationFilter.getComponents().isEmpty();
    }

    public ElementAggregator getAggregator() {
        return aggregator;
    }

    public void setAggregator(final ElementAggregator aggregator) {
        this.aggregator = aggregator;
    }

    @JsonIgnore
    public ElementFilter getPostAggregationFilter() {
        return postAggregationFilter;
    }

    @JsonGetter("postAggregationFilterFunctions")
    public List<TupleAdaptedPredicate<String, ?>> getPostAggregationFilterFunctions() {
        return null != postAggregationFilter ? postAggregationFilter.getComponents() : null;
    }

    public boolean hasPostAggregationFilters() {
        return null != postAggregationFilter && !postAggregationFilter.getComponents().isEmpty();
    }

    @JsonIgnore
    public ElementFilter getPostTransformFilter() {
        return postTransformFilter;
    }

    @JsonGetter("postTransformFilterFunctions")
    public List<TupleAdaptedPredicate<String, ?>> getPostTransformFilterFunctions() {
        return null != postTransformFilter ? postTransformFilter.getComponents() : null;
    }

    public boolean hasPostTransformFilters() {
        return null != postTransformFilter && !postTransformFilter.getComponents().isEmpty();
    }

    @JsonIgnore
    public ElementTransformer getTransformer() {
        return transformer;
    }

    @JsonGetter("transformFunctions")
    public List<TupleAdaptedFunction<String, ?, ?>> getTransformFunctions() {
        return null != transformer ? transformer.getComponents() : null;
    }

    @SuppressWarnings("CloneDoesntCallSuperClone")
    @SuppressFBWarnings(value = "CN_IDIOM_NO_SUPER_CALL", justification = "Only inherits from Object")
    @Override
    public ViewElementDefinition clone() {
        return new ViewElementDefinition.Builder().json(toJson(false)).build();
    }

    public byte[] toJson(final boolean prettyPrint, final String... fieldsToExclude) throws SchemaException {
        try {
            return JSONSerialiser.serialise(this, prettyPrint, fieldsToExclude);
        } catch (final SerialisationException e) {
            throw new SchemaException(e.getMessage(), e);
        }
    }

    public byte[] toCompactJson() throws SchemaException {
        return toJson(false);
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final ViewElementDefinition that = (ViewElementDefinition) obj;

        return new EqualsBuilder()
                .append(transformer, that.transformer)
                .append(preAggregationFilter, that.preAggregationFilter)
                .append(postAggregationFilter, that.postAggregationFilter)
                .append(postTransformFilter, that.postTransformFilter)
                .append(groupBy, that.groupBy)
                .append(transientProperties, that.transientProperties)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(7, 23)
                .append(transformer)
                .append(preAggregationFilter)
                .append(postAggregationFilter)
                .append(postTransformFilter)
                .append(groupBy)
                .append(transientProperties)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("transformer", transformer)
                .append("preAggregationFilter", preAggregationFilter)
                .append("postAggregationFilter", postAggregationFilter)
                .append("postTransformFilter", postTransformFilter)
                .append("groupBy", groupBy)
                .append("transientProperties", transientProperties)
                .toString();
    }

    @Override
    public void lock() {
        if (null != groupBy) {
            groupBy = Collections.unmodifiableSet(groupBy);
        }

        transientProperties = Collections.unmodifiableMap(transientProperties);
    }

    public abstract static class BaseBuilder<CHILD_CLASS extends BaseBuilder<?>> {
        private final ViewElementDefinition elDef;

        public BaseBuilder() {
            this.elDef = new ViewElementDefinition();
        }

        protected BaseBuilder(final ViewElementDefinition elementDef) {
            this.elDef = elementDef;
        }

        public CHILD_CLASS allProperties() {
            elDef.properties = null;
            elDef.excludeProperties = null;
            return self();
        }

        @JsonSetter("properties")
        public CHILD_CLASS properties(final Set<String> properties) {
            if (null != properties && null != elDef.excludeProperties && !elDef.excludeProperties.isEmpty()) {
                throw new IllegalArgumentException("You cannot set both properties and excludeProperties");
            }
            elDef.properties = properties;
            return self();
        }

        public CHILD_CLASS properties(final String... properties) {
            if (null != properties && null != elDef.excludeProperties && !elDef.excludeProperties.isEmpty()) {
                throw new IllegalArgumentException("You cannot set both properties and excludeProperties");
            }

            if (null == properties) {
                elDef.properties = null;
            } else {
                elDef.properties = Sets.newHashSet(properties);
            }
            return self();
        }

        @JsonSetter("excludeProperties")
        public CHILD_CLASS excludeProperties(final Set<String> excludeProperties) {
            if (null != excludeProperties && !excludeProperties.isEmpty() && null != elDef.properties) {
                throw new IllegalArgumentException("You cannot set both properties and excludeProperties");
            }

            elDef.excludeProperties = excludeProperties;
            return self();
        }

        public CHILD_CLASS excludeProperties(final String... excludeProperties) {
            if (null != excludeProperties && excludeProperties.length > 0 && null != elDef.properties) {
                throw new IllegalArgumentException("You cannot set both properties and excludeProperties");
            }

            if (null == excludeProperties) {
                elDef.excludeProperties = null;
            } else {
                elDef.excludeProperties = Sets.newHashSet(excludeProperties);
            }
            return self();
        }

        public CHILD_CLASS transientProperty(final String propertyName, final Class<?> clazz) {
            elDef.transientProperties.put(propertyName, clazz);
            return self();
        }

        public CHILD_CLASS transientProperties(final Map<String, Class<?>> transientProperties) {
            if (null == transientProperties) {
                elDef.transientProperties = new LinkedHashMap<>();
            } else {
                elDef.transientProperties = new LinkedHashMap<>(transientProperties);
            }
            return self();
        }

        public CHILD_CLASS preAggregationFilter(final ElementFilter preAggregationFilter) {
            if (null != getElementDef().getPreAggregationFilter()) {
                throw new IllegalArgumentException("ViewElementDefinition.Builder().preAggregationFilter(ElementFilter)" +
                        "may only be called once.");
            }

            getElementDef().preAggregationFilter = preAggregationFilter;
            return self();
        }

        public CHILD_CLASS preAggregationFilterFunctions(final List<TupleAdaptedPredicate<String, ?>> filterFunctions) {
            getElementDef().preAggregationFilter = new ElementFilter();
            if (null != filterFunctions) {
                getElementDef().preAggregationFilter.getComponents().addAll(filterFunctions);
            }
            return self();
        }

        public CHILD_CLASS aggregator(final ElementAggregator aggregator) {
            getElementDef().aggregator = aggregator;
            return self();
        }

        public CHILD_CLASS postAggregationFilter(final ElementFilter postAggregationFilter) {
            if (null != getElementDef().getPostAggregationFilter()) {
                throw new IllegalArgumentException("ViewElementDefinition.Builder().postAggregationFilter(ElementFilter)" +
                        "may only be called once.");
            }

            getElementDef().postAggregationFilter = postAggregationFilter;
            return self();
        }

        public CHILD_CLASS postAggregationFilterFunctions(final List<TupleAdaptedPredicate<String, ?>> filterFunctions) {
            getElementDef().postAggregationFilter = new ElementFilter();
            if (null != filterFunctions) {
                getElementDef().postAggregationFilter.getComponents().addAll(filterFunctions);
            }
            return self();
        }

        public CHILD_CLASS postTransformFilter(final ElementFilter postTransformFilter) {
            if (null != getElementDef().getPostTransformFilter()) {
                throw new IllegalArgumentException("ViewElementDefinition.Builder().postTransformFilter(ElementFilter)" +
                        "may only be called once.");
            }

            getElementDef().postTransformFilter = postTransformFilter;
            return self();
        }

        public CHILD_CLASS postTransformFilterFunctions(final List<TupleAdaptedPredicate<String, ?>> filterFunctions) {
            getElementDef().postTransformFilter = new ElementFilter();
            if (null != filterFunctions) {
                getElementDef().postTransformFilter.getComponents().addAll(filterFunctions);
            }
            return self();
        }

        public CHILD_CLASS transformer(final ElementTransformer transformer) {
            getElementDef().transformer = transformer;
            return self();
        }

        public CHILD_CLASS transformFunctions(final List<TupleAdaptedFunction<String, ?, ?>> transformFunctions) {
            getElementDef().transformer = new ElementTransformer();
            if (null != transformFunctions) {
                getElementDef().transformer.getComponents().addAll(transformFunctions);
            }
            return self();
        }

        public CHILD_CLASS groupBy(final String... groupBy) {
            if (null != groupBy) {
                if (null == getElementDef().getGroupBy()) {
                    getElementDef().setGroupBy(new LinkedHashSet<>());
                }
                Collections.addAll(getElementDef().getGroupBy(), groupBy);
            } else {
                getElementDef().setGroupBy(null);
            }
            return self();
        }

        @JsonIgnore
        public CHILD_CLASS json(final byte[] jsonBytes) throws SchemaException {
            return json(jsonBytes, ViewElementDefinition.class);
        }

        @JsonIgnore
        protected CHILD_CLASS json(final byte[] jsonBytes, final Class<? extends ViewElementDefinition> clazz) throws SchemaException {
            if (null != jsonBytes) {
                try {
                    merge(JSONSerialiser.deserialise(jsonBytes, clazz));
                } catch (final SerialisationException e) {
                    throw new SchemaException("Unable to deserialise json", e);
                }
            }
            return self();
        }

        public CHILD_CLASS merge(final ViewElementDefinition elementDef) {
            if (null != elementDef) {
                for (final Entry<String, Class<?>> entry : elementDef.getTransientPropertyMap().entrySet()) {
                    final String newProp = entry.getKey();
                    final Class<?> newPropClass = entry.getValue();
                    if (!getElementDef().transientProperties.containsKey(newProp)) {
                        getElementDef().transientProperties.put(newProp, newPropClass);
                    } else {
                        final Class<?> clazz = getElementDef().transientProperties.get(newProp);
                        if (!clazz.equals(newPropClass)) {
                            throw new SchemaException("Unable to merge schemas. Conflict of transient property classes for " + newProp
                                    + ". Classes are: " + clazz.getName() + " and " + newPropClass.getName());
                        }
                    }
                }

                if (null == getElementDef().preAggregationFilter) {
                    getElementDef().preAggregationFilter = elementDef.preAggregationFilter;
                } else if (null != elementDef.preAggregationFilter) {
                    getElementDef().preAggregationFilter.getComponents().addAll(elementDef.preAggregationFilter.getComponents());
                }

                if (null == getElementDef().postAggregationFilter) {
                    getElementDef().postAggregationFilter = elementDef.postAggregationFilter;
                } else if (null != elementDef.postAggregationFilter) {
                    getElementDef().postAggregationFilter.getComponents().addAll(elementDef.postAggregationFilter.getComponents());
                }

                if (null == getElementDef().postTransformFilter) {
                    getElementDef().postTransformFilter = elementDef.postTransformFilter;
                } else if (null != elementDef.postTransformFilter) {
                    getElementDef().postTransformFilter.getComponents().addAll(elementDef.postTransformFilter.getComponents());
                }

                if (null == getElementDef().transformer) {
                    getElementDef().transformer = elementDef.transformer;
                } else if (null != elementDef.transformer) {
                    getElementDef().transformer.getComponents().addAll(elementDef.transformer.getComponents());
                }

                if (null != elementDef.getGroupBy()) {
                    getElementDef().groupBy = new LinkedHashSet<>(elementDef.getGroupBy());
                }

                if (null != elementDef.getProperties()) {
                    properties(elementDef.getProperties());
                }

                if (null != elementDef.getExcludeProperties() && !elementDef.getExcludeProperties().isEmpty()) {
                    excludeProperties(elementDef.getExcludeProperties());
                }
            }

            return self();
        }

        public ViewElementDefinition build() {
            elDef.lock();
            return elDef;
        }

        public ViewElementDefinition getElementDef() {
            return elDef;
        }

        protected abstract CHILD_CLASS self();
    }

    @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
    public static final class Builder extends BaseBuilder<Builder> {
        public Builder() {
        }

        public Builder(final ViewElementDefinition viewElementDef) {
            this();
            merge(viewElementDef);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
