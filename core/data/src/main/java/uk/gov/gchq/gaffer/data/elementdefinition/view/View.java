/*
 * Copyright 2016-2023 Crown Copyright
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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.data.elementdefinition.ElementDefinitions;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.koryphe.serialisation.json.JsonSimpleClassName;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = As.EXISTING_PROPERTY, property = "class", defaultImpl = View.class)
@JsonPropertyOrder(value = {"class", "edges", "entities", "allEdges", "allEntities", "globalElements", "globalEntities", "globalEdges"}, alphabetic = true)
@JsonSimpleClassName(includeSubtypes = true)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class View extends ElementDefinitions<ViewElementDefinition, ViewElementDefinition> implements Cloneable {
    private List<GlobalViewElementDefinition> globalElements;
    private List<GlobalViewElementDefinition> globalEntities;
    private List<GlobalViewElementDefinition> globalEdges;
    private final Map<String, String> config = new HashMap<>();
    private boolean allEntities = false;
    private boolean allEdges = false;

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
        return new ToStringBuilder(this)
                .append(new String(toJson(true), StandardCharsets.UTF_8))
                .build();
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

    public boolean isAllEntities() {
        return allEntities;
    }

    public void setAllEntities(final boolean allEntities) {
        this.allEntities = allEntities;
    }

    public boolean isAllEdges() {
        return allEdges;
    }

    public void setAllEdges(final boolean allEdges) {
        this.allEdges = allEdges;
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

    public boolean hasTransform() {
        return hasFilters(ViewElementDefinition::hasTransform);
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
        if (null != globalEntities && !globalEntities.isEmpty() && !getEntityGroups().isEmpty()) {
            setEntities(expandGlobalDefinitions(getEntities(), getEntityGroups(), globalEntities, false));
            globalEntities = null;
        }

        if (null != globalEdges && !globalEdges.isEmpty() && !getEdgeGroups().isEmpty()) {
            setEdges(expandGlobalDefinitions(getEdges(), getEdgeGroups(), globalEdges, false));
            globalEdges = null;
        }

        if (null != globalElements && !globalElements.isEmpty() && (!getEdgeGroups().isEmpty() || !getEntityGroups().isEmpty())) {
            setEntities(expandGlobalDefinitions(getEntities(), getEntityGroups(), globalElements, true));
            setEdges(expandGlobalDefinitions(getEdges(), getEdgeGroups(), globalElements, true));
            globalElements = null;
        }
    }

    @JsonInclude(Include.NON_EMPTY)
    public Map<String, String> getConfig() {
        return this.config;
    }

    public void addConfig(final String key, final String value) {
        if (!this.config.containsKey(key)) {
            this.config.put(key, value);
        }
    }

    public String getConfig(final String key) {
        return this.config.get(key);
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

    public boolean canMerge(final View addingView, final View srcView) {
        if (addingView != null && srcView == null) {
            return false;
        }
        return true;
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
                .append(allEntities, view.isAllEntities())
                .append(allEdges, view.isAllEdges())
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .appendSuper(super.hashCode())
                .append(globalElements)
                .append(globalEntities)
                .append(globalEdges)
                .append(allEntities)
                .append(allEdges)
                .toHashCode();
    }

    @JsonInclude(Include.NON_NULL)
    @JsonGetter("class")
    public String getClassName() {
        return View.class.equals(getClass()) ? null : getClass().getName();
    }

    @JsonSetter("class")
    void setClassName(final String className) {
        // ignore the className as it will be picked up by the JsonTypeInfo annotation.
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

        @Override
        public CHILD_CLASS entity(final String group, final ViewElementDefinition entityDef) {
            return super.entity(group, null != entityDef ? entityDef : new ViewElementDefinition());
        }

        @JsonIgnore
        public CHILD_CLASS entities(final Collection<String> groups) {
            if (null != groups) {
                for (final String group : groups) {
                    entity(group);
                }
            }

            return self();
        }

        public CHILD_CLASS allEntities(final boolean allEntities) {
            getThisView().allEntities = allEntities;
            return self();
        }

        public CHILD_CLASS edge(final String group) {
            return edge(group, new ViewElementDefinition());
        }

        @Override
        public CHILD_CLASS edge(final String group, final ViewElementDefinition edgeDef) {
            return super.edge(group, null != edgeDef ? edgeDef : new ViewElementDefinition());
        }

        public CHILD_CLASS edges(final Collection<String> groups) {
            if (null != groups) {
                for (final String group : groups) {
                    edge(group);
                }
            }

            return self();
        }

        public CHILD_CLASS allEdges(final boolean allEdges) {
            getThisView().allEdges = allEdges;
            return self();
        }

        public CHILD_CLASS config(final String key, final String value) {
            getThisView().config.put(key, value);
            return self();
        }

        public CHILD_CLASS config(final Map<String, String> config) {
            if (null != config) {
                getThisView().config.putAll(config);
            }
            return self();
        }

        public CHILD_CLASS globalElements(final GlobalViewElementDefinition... globalElements) {
            if (null != globalElements && globalElements.length > 0) {
                if (null == getThisView().globalElements) {
                    getThisView().globalElements = new ArrayList<>();
                }
                Collections.addAll(getThisView().globalElements, globalElements);
            }
            return self();
        }

        public CHILD_CLASS globalEntities(final GlobalViewElementDefinition... globalEntities) {
            if (null != globalEntities && globalEntities.length > 0) {
                if (null == getThisView().globalEntities) {
                    getThisView().globalEntities = new ArrayList<>();
                }
                Collections.addAll(getThisView().globalEntities, globalEntities);
            }
            return self();
        }

        public CHILD_CLASS globalEdges(final GlobalViewElementDefinition... globalEdges) {
            if (null != globalEdges && globalEdges.length > 0) {
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
            if (null != view) {
                if (!(getThisView().canMerge(view, getThisView()) && view.canMerge(view, getThisView()))) {
                    throw new IllegalArgumentException("A " + view.getClass().getSimpleName() +
                            " cannot be merged into a " + getThisView().getClass().getSimpleName());
                }

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

                if (null != view.config) {
                    getThisView().config.putAll(view.config);
                }

                getThisView().setAllEntities(view.allEntities);
                getThisView().setAllEdges(view.allEdges);
            }

            return self();
        }

        public CHILD_CLASS expandGlobalDefinitions() {
            getThisView().expandGlobalDefinitions();
            return self();
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
