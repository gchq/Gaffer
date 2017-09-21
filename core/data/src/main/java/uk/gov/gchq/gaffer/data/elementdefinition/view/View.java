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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.data.elementdefinition.ElementDefinitions;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * The {@code View} defines the {@link uk.gov.gchq.gaffer.data.element.Element}s to be returned for an operation.
 * A view should contain {@link uk.gov.gchq.gaffer.data.element.Edge} and {@link uk.gov.gchq.gaffer.data.element.Entity} types required and
 * for each group it can optionally contain an {@link uk.gov.gchq.gaffer.data.element.function.ElementFilter} and a
 * {@link uk.gov.gchq.gaffer.data.element.function.ElementTransformer}.
 * The {@link java.util.function.Predicate}s within the ElementFilter describe the how the elements should be filtered.
 * The {@link java.util.function.Function}s within ElementTransformer allow transient properties to be created
 * from other properties and identifiers.
 * It also contains any transient properties that are created in transform functions.
 *
 * @see Builder
 * @see uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition
 * @see uk.gov.gchq.gaffer.data.element.function.ElementFilter
 * @see uk.gov.gchq.gaffer.data.element.function.ElementTransformer
 */
@JsonDeserialize(builder = View.Builder.class)
public class View extends ElementDefinitions<ViewElementDefinition, ViewElementDefinition> implements Cloneable {
    private List<GlobalViewElementDefinition> globalElements;
    private List<GlobalViewElementDefinition> globalEntities;
    private List<GlobalViewElementDefinition> globalEdges;

    public View() {
        super();
    }

    public static View fromJson(final InputStream inputStream) throws SchemaException {
        return new View.Builder().json(inputStream).build();
    }

    public static View fromJson(final Path filePath) throws SchemaException {
        return new View.Builder().json(filePath).build();
    }

    public static View fromJson(final byte[] jsonBytes) throws SchemaException {
        return new View.Builder().json(jsonBytes).build();
    }

    public byte[] toCompactJson() throws SchemaException {
        return toJson(false);
    }

    @Override
    public String toString() {
        try {
            return new ToStringBuilder(this)
                    .append(new String(toJson(true), CommonConstants.UTF_8))
                    .build();
        } catch (final UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ViewElementDefinition getElement(final String group) {
        return (ViewElementDefinition) super.getElement(group);
    }

    public Set<String> getElementGroupBy(final String group) {
        ViewElementDefinition viewElementDef = (ViewElementDefinition) super.getElement(group);
        if (null == viewElementDef) {
            return null;
        }

        return viewElementDef.getGroupBy();
    }

    public List<GlobalViewElementDefinition> getGlobalElements() {
        return globalElements;
    }

    public List<GlobalViewElementDefinition> getGlobalEntities() {
        return globalEntities;
    }

    public List<GlobalViewElementDefinition> getGlobalEdges() {
        return globalEdges;
    }

    public boolean hasPreAggregationFilters() {
        return hasFilters(ViewElementDefinition::hasPreAggregationFilters);
    }

    public boolean hasPostAggregationFilters() {
        return hasFilters(ViewElementDefinition::hasPostAggregationFilters);
    }

    public boolean hasPostTransformFilters() {
        return hasFilters(ViewElementDefinition::hasPostTransformFilters);
    }

    public boolean hasEntityFilters() {
        return hasEntityFilters(ViewElementDefinition::hasPostAggregationFilters)
                || hasEntityFilters(ViewElementDefinition::hasPostTransformFilters)
                || hasEntityFilters(ViewElementDefinition::hasPreAggregationFilters);
    }

    public boolean hasEdgeFilters() {
        return hasEdgeFilters(ViewElementDefinition::hasPostAggregationFilters)
                || hasEdgeFilters(ViewElementDefinition::hasPostTransformFilters)
                || hasEdgeFilters(ViewElementDefinition::hasPreAggregationFilters);
    }


    @SuppressWarnings("CloneDoesntCallSuperClone")
    @SuppressFBWarnings(value = "CN_IDIOM_NO_SUPER_CALL", justification = "Only inherits from Object")
    @Override
    public View clone() {
        return fromJson(toJson(false));
    }

    @Override
    protected void lock() {
        super.lock();
        if (null != globalElements) {
            globalElements = Collections.unmodifiableList(globalElements);
        }

        if (null != globalEntities) {
            globalEntities = Collections.unmodifiableList(globalEntities);
        }

        if (null != globalEdges) {
            globalEdges = Collections.unmodifiableList(globalEdges);
        }
    }

    /**
     * Copies all the global element definitions into the individual element definitions.
     * The global element definitions will then be set to null
     */
    public void expandGlobalDefinitions() {
        if (null != globalEntities && !globalEntities.isEmpty()) {
            setEntities(expandGlobalDefinitions(getEntities(), getEntityGroups(), globalEntities, false));
            globalEntities = null;
        }

        if (null != globalEdges && !globalEdges.isEmpty()) {
            setEdges(expandGlobalDefinitions(getEdges(), getEdgeGroups(), globalEdges, false));
            globalEdges = null;
        }

        if (null != globalElements && !globalElements.isEmpty()) {
            setEntities(expandGlobalDefinitions(getEntities(), getEntityGroups(), globalElements, true));
            setEdges(expandGlobalDefinitions(getEdges(), getEdgeGroups(), globalElements, true));
            globalElements = null;
        }
    }

    private Map<String, ViewElementDefinition> expandGlobalDefinitions(
            final Map<String, ViewElementDefinition> elements,
            final Set<String> groups,
            final List<GlobalViewElementDefinition> globalElements,
            final boolean skipMissingGroups) {

        final Map<String, ViewElementDefinition> newElements = new LinkedHashMap<>();
        for (final GlobalViewElementDefinition globalElement : globalElements) {
            final Set<String> globalGroups;
            if (null != globalElement.groups) {
                globalGroups = new HashSet<>(globalElement.groups);
                final boolean hasMissingGroups = globalGroups.retainAll(groups);
                if (hasMissingGroups && !skipMissingGroups) {
                    final Set<String> missingGroups = new HashSet<>(globalElement.groups);
                    missingGroups.removeAll(groups);
                    throw new IllegalArgumentException("A global element definition is invalid, these groups do not exist: " + missingGroups);
                }
            } else {
                globalGroups = groups;
            }
            for (final String group : globalGroups) {
                final ViewElementDefinition.Builder builder = new ViewElementDefinition.Builder();
                if (newElements.containsKey(group)) {
                    builder.merge(newElements.get(group));
                }
                builder.merge(globalElement.clone());
                newElements.put(group, builder.build());
            }
        }

        if (null != elements) {
            for (final Map.Entry<String, ViewElementDefinition> entry : elements.entrySet()) {
                final String group = entry.getKey();
                if (newElements.containsKey(group)) {
                    newElements.put(group, new ViewElementDefinition.Builder()
                            .merge(newElements.get(group))
                            .merge(entry.getValue())
                            .build());
                } else {
                    newElements.put(group, entry.getValue());
                }
            }
        }

        return Collections.unmodifiableMap(newElements);
    }

    private boolean hasFilters(final Function<ViewElementDefinition, Boolean> hasFilters) {
        return hasEdgeFilters(hasFilters) || hasEntityFilters(hasFilters);
    }

    private boolean hasEntityFilters(final Function<ViewElementDefinition, Boolean> hasEntityFilters) {
        for (final ViewElementDefinition value : getEntities().values()) {
            if (null != value && hasEntityFilters.apply(value)) {
                return true;
            }
        }
        return false;
    }

    private boolean hasEdgeFilters(final Function<ViewElementDefinition, Boolean> hasEdgeFilters) {
        for (final ViewElementDefinition value : getEdges().values()) {
            if (null != value && hasEdgeFilters.apply(value)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final View view = (View) obj;

        return new EqualsBuilder()
                .appendSuper(super.equals(view))
                .append(globalElements, view.getGlobalElements())
                .append(globalEntities, view.getGlobalEntities())
                .append(globalEdges, view.getGlobalEdges())
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .appendSuper(super.hashCode())
                .append(globalElements)
                .append(globalEntities)
                .append(globalEdges)
                .toHashCode();
    }

    public abstract static class BaseBuilder<CHILD_CLASS extends BaseBuilder<?>> extends ElementDefinitions.BaseBuilder<View, ViewElementDefinition, ViewElementDefinition, CHILD_CLASS> {
        public BaseBuilder() {
            super(new View());
        }

        protected BaseBuilder(final View view) {
            super(view);
        }

        public CHILD_CLASS entity(final String group) {
            return entity(group, new ViewElementDefinition());
        }

        @JsonIgnore
        public CHILD_CLASS entities(final Collection<String> groups) {
            for (final String group : groups) {
                entity(group);
            }

            return self();
        }

        public CHILD_CLASS edge(final String group) {
            return edge(group, new ViewElementDefinition());
        }

        public CHILD_CLASS edges(final Collection<String> groups) {
            for (final String group : groups) {
                edge(group);
            }

            return self();
        }

        public CHILD_CLASS globalElements(final GlobalViewElementDefinition... globalElements) {
            if (globalElements.length > 0) {
                if (null == getThisView().globalElements) {
                    getThisView().globalElements = new ArrayList<>();
                }
                Collections.addAll(getThisView().globalElements, globalElements);
            }
            return self();
        }

        public CHILD_CLASS globalEntities(final GlobalViewElementDefinition... globalEntities) {
            if (globalEntities.length > 0) {
                if (null == getThisView().globalEntities) {
                    getThisView().globalEntities = new ArrayList<>();
                }
                Collections.addAll(getThisView().globalEntities, globalEntities);
            }
            return self();
        }

        public CHILD_CLASS globalEdges(final GlobalViewElementDefinition... globalEdges) {
            if (globalEdges.length > 0) {
                if (null == getThisView().globalEdges) {
                    getThisView().globalEdges = new ArrayList<>();
                }
                Collections.addAll(getThisView().globalEdges, globalEdges);
            }
            return self();
        }

        @JsonIgnore
        public CHILD_CLASS json(final InputStream... inputStreams) throws SchemaException {
            return json(View.class, inputStreams);
        }

        @JsonIgnore
        public CHILD_CLASS json(final Path... filePaths) throws SchemaException {
            return json(View.class, filePaths);
        }

        @JsonIgnore
        public CHILD_CLASS json(final byte[]... jsonBytes) throws SchemaException {
            return json(View.class, jsonBytes);
        }

        @Override
        @JsonIgnore
        public CHILD_CLASS merge(final View view) {
            if (getThisView().getEntities().isEmpty()) {
                getThisView().getEntities().putAll(view.getEntities());
            } else {
                for (final Map.Entry<String, ViewElementDefinition> entry : view.getEntities().entrySet()) {
                    if (!getThisView().getEntities().containsKey(entry.getKey())) {
                        entity(entry.getKey(), entry.getValue());
                    } else {
                        final ViewElementDefinition mergedElementDef = new ViewElementDefinition.Builder()
                                .merge(getThisView().getEntities().get(entry.getKey()))
                                .merge(entry.getValue())
                                .build();
                        getThisView().getEntities().put(entry.getKey(), mergedElementDef);
                    }
                }
            }

            if (getThisView().getEdges().isEmpty()) {
                getThisView().getEdges().putAll(view.getEdges());
            } else {
                for (final Map.Entry<String, ViewElementDefinition> entry : view.getEdges().entrySet()) {
                    if (!getThisView().getEdges().containsKey(entry.getKey())) {
                        edge(entry.getKey(), entry.getValue());
                    } else {
                        final ViewElementDefinition mergedElementDef = new ViewElementDefinition.Builder()
                                .merge(getThisView().getEdges().get(entry.getKey()))
                                .merge(entry.getValue())
                                .build();
                        getThisView().getEdges().put(entry.getKey(), mergedElementDef);
                    }
                }
            }

            if (null != view.globalElements) {
                if (null == getThisView().globalElements) {
                    getThisView().globalElements = new ArrayList<>();
                }
                getThisView().globalElements.addAll(view.globalElements);
            }

            if (null != view.globalEntities) {
                if (null == getThisView().globalEntities) {
                    getThisView().globalEntities = new ArrayList<>();
                }
                getThisView().globalEntities.addAll(view.globalEntities);
            }

            if (null != view.globalEdges) {
                if (null == getThisView().globalEdges) {
                    getThisView().globalEdges = new ArrayList<>();
                }
                getThisView().globalEdges.addAll(view.globalEdges);
            }

            return self();
        }

        @Override
        public View build() {
            return super.build();
        }

        private View getThisView() {
            return getElementDefs();
        }
    }

    @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
    public static final class Builder extends BaseBuilder<Builder> {
        public Builder() {
        }

        public Builder(final View view) {
            this();
            merge(view);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
