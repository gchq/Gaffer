/*
 * Copyright 2018 Crown Copyright
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

package uk.gov.gchq.gaffer.graph.hook.migrate;

import org.apache.commons.collections.CollectionUtils;

import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.impl.function.Aggregate;
import uk.gov.gchq.gaffer.operation.impl.function.Filter;
import uk.gov.gchq.gaffer.operation.impl.function.Transform;
import uk.gov.gchq.koryphe.util.IterableUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * POJO to hold all relevant ElementFilters and ElementTransformers with the related ViewElementDefinition.
 */
public class MigratedView {
    private final boolean aggregateAfter;
    private ViewElementDefinition viewElementDefinition;
    private Map<String, ElementFilter> entitiesPostAggregationFilterMap;
    private Map<String, ElementFilter> edgesPostAggregationFilterMap;
    private Map<String, ElementTransformer> entitiesTransformerMap;
    private Map<String, ElementTransformer> edgesTransformerMap;
    private Map<String, ElementFilter> entitiesPostTransformFilterMap;
    private Map<String, ElementFilter> edgesPostTransformFilterMap;

    public MigratedView(final boolean aggregateAfter) {
        this.aggregateAfter = aggregateAfter;
        viewElementDefinition = new ViewElementDefinition();
        entitiesPostAggregationFilterMap = new HashMap<>();
        edgesPostAggregationFilterMap = new HashMap<>();
        entitiesTransformerMap = new HashMap<>();
        edgesTransformerMap = new HashMap<>();
        entitiesPostTransformFilterMap = new HashMap<>();
        edgesPostTransformFilterMap = new HashMap<>();
    }

    public List<Operation> createMigrationOps(final MigratedView migratedView) {
        final List<Operation> updatedOps = new ArrayList<>();
        if (aggregateAfter) {
            final Aggregate aggregate = new Aggregate();
            updatedOps.add(aggregate);
        }

        if (!migratedView.getEntitiesPostAggregationFilterMap().isEmpty()
                || !migratedView.getEdgesPostAggregationFilterMap().isEmpty()) {
            final Filter postAggregationFilter = new Filter.Builder()
                    .entities(migratedView.getEntitiesPostAggregationFilterMap())
                    .edges(migratedView.getEdgesPostAggregationFilterMap())
                    .build();
            updatedOps.add(postAggregationFilter);
        }

        if (!migratedView.getEntitiesTransformerMap().isEmpty()
                || !migratedView.getEdgesTransformerMap().isEmpty()) {
            final Transform transformFunction = new Transform.Builder()
                    .entities(migratedView.getEntitiesTransformerMap())
                    .edges(migratedView.getEdgesTransformerMap())
                    .build();
            updatedOps.add(transformFunction);
        }

        if (!migratedView.getEdgesPostTransformFilterMap().isEmpty()
                || !migratedView.getEntitiesPostTransformFilterMap().isEmpty()) {
            final Filter postTransformFilter = new Filter.Builder()
                    .entities(migratedView.getEntitiesPostTransformFilterMap())
                    .edges(migratedView.getEdgesPostTransformFilterMap())
                    .build();
            updatedOps.add(postTransformFilter);
        }

        return updatedOps;
    }

    public void update(final MigrateElement.ElementType migrationElementType, final String migrationGroup, final ViewElementDefinition elementDefinition) {
        if (CollectionUtils.isNotEmpty(elementDefinition.getPostAggregationFilterFunctions())) {
            updatePostAggregationFilters(migrationElementType, migrationGroup, elementDefinition.getPostAggregationFilter());
            getViewElementDefinition().setPostAggregationFilter(null);
        }
        if (CollectionUtils.isNotEmpty(elementDefinition.getPostTransformFilterFunctions())) {
            updatePostTransformFilters(migrationElementType, migrationGroup, elementDefinition.getPostTransformFilter());
            getViewElementDefinition().setPostTransformFilter(null);
        }
        if (CollectionUtils.isNotEmpty(elementDefinition.getTransformFunctions())) {
            updateTransformerMap(migrationElementType, migrationGroup, elementDefinition.getTransformer());
            getViewElementDefinition().setTransformer(null);
        }
    }

    public void updatePostTransformFilters(final MigrateElement.ElementType migrationElementType, final String migrationGroup, final ElementFilter filter) {
        if (CollectionUtils.isNotEmpty(filter.getComponents())) {
            if (migrationElementType.equals(MigrateElement.ElementType.ENTITY)) {
                getEntitiesPostTransformFilterMap().put(migrationGroup, filter);
            } else if (migrationElementType.equals(MigrateElement.ElementType.EDGE)) {
                getEdgesPostTransformFilterMap().put(migrationGroup, filter);
            }
        }
    }

    public void updatePostAggregationFilters(final MigrateElement.ElementType migrationElementType, final String migrationGroup, final ElementFilter filter) {
        if (CollectionUtils.isNotEmpty(filter.getComponents())) {
            if (migrationElementType.equals(MigrateElement.ElementType.ENTITY)) {
                getEntitiesPostAggregationFilterMap().put(migrationGroup, filter);
            } else if (migrationElementType.equals(MigrateElement.ElementType.EDGE)) {
                getEdgesPostAggregationFilterMap().put(migrationGroup, filter);
            }
        }
    }

    public void updateTransformerMap(final MigrateElement.ElementType migrationElementType, final String migrationGroup, final ElementTransformer transformer) {
        if (CollectionUtils.isNotEmpty(transformer.getComponents())) {
            if (migrationElementType.equals(MigrateElement.ElementType.ENTITY)) {
                getEntitiesTransformerMap().put(migrationGroup, transformer);
            } else if (migrationElementType.equals(MigrateElement.ElementType.EDGE)) {
                getEdgesTransformerMap().put(migrationGroup, transformer);
            }
        }
    }

    public ViewElementDefinition getViewElementDefinition() {
        return viewElementDefinition;
    }

    public void setViewElementDefinition(final ViewElementDefinition viewElementDefinition) {
        this.viewElementDefinition = viewElementDefinition;
    }

    public Map<String, ElementFilter> getEntitiesPostAggregationFilterMap() {
        return entitiesPostAggregationFilterMap;
    }

    public Map<String, ElementFilter> getEdgesPostAggregationFilterMap() {
        return edgesPostAggregationFilterMap;
    }

    public Map<String, ElementTransformer> getEntitiesTransformerMap() {
        return entitiesTransformerMap;
    }

    public Map<String, ElementTransformer> getEdgesTransformerMap() {
        return edgesTransformerMap;
    }

    public Map<String, ElementFilter> getEntitiesPostTransformFilterMap() {
        return entitiesPostTransformFilterMap;
    }

    public Map<String, ElementFilter> getEdgesPostTransformFilterMap() {
        return edgesPostTransformFilterMap;
    }

    public static MigratedView merge(final boolean aggregateAfter, final Iterable<MigratedView> views1, final Iterable<MigratedView> views2) {
        return merge(aggregateAfter, IterableUtil.concat(Arrays.asList(views1, views2)));
    }

    public static MigratedView merge(final boolean aggregateAfter, final Iterable<MigratedView> views) {
        final MigratedView migratedView = new MigratedView(aggregateAfter);
        for (final MigratedView view : views) {
            migratedView.edgesPostAggregationFilterMap.putAll(view.getEdgesPostAggregationFilterMap());
            migratedView.entitiesPostAggregationFilterMap.putAll(view.getEntitiesPostAggregationFilterMap());
            migratedView.edgesPostTransformFilterMap.putAll(view.getEdgesPostTransformFilterMap());
            migratedView.entitiesPostTransformFilterMap.putAll(view.getEntitiesPostTransformFilterMap());
            migratedView.edgesTransformerMap.putAll(view.getEdgesTransformerMap());
            migratedView.entitiesTransformerMap.putAll(view.getEntitiesTransformerMap());
        }
        return migratedView;
    }
}
