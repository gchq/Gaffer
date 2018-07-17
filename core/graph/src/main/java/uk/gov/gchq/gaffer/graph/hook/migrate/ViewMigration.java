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

import uk.gov.gchq.gaffer.data.element.ElementTuple;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.graph.hook.migrate.predicate.TransformAndFilter;
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

import static org.apache.commons.collections.CollectionUtils.isNotEmpty;

/**
 * POJO to hold all relevant ElementFilters and ElementTransformers with the related ViewElementDefinition.
 */
public class ViewMigration {
    private final Map<String, ElementFilter> entitiesPostAggregationFilter = new HashMap<>();
    private final Map<String, ElementFilter> edgesPostAggregationFilter = new HashMap<>();
    private final Map<String, ElementTransformer> entitiesTransformer = new HashMap<>();
    private final Map<String, ElementTransformer> edgesTransformer = new HashMap<>();
    private final Map<String, ElementFilter> entitiesPostTransformFilter = new HashMap<>();
    private final Map<String, ElementFilter> edgesPostTransformFilter = new HashMap<>();

    private ViewElementDefinition viewElementDefinition = new ViewElementDefinition();

    public static List<Operation> createMigrationOps(final boolean aggregateAfter, final Iterable<ViewMigration> views1, final Iterable<ViewMigration> views2) {
        return createMigrationOps(aggregateAfter, IterableUtil.concat(Arrays.asList(views1, views2)));
    }

    public static List<Operation> createMigrationOps(final boolean aggregateAfter, final Iterable<ViewMigration> views) {
        final ViewMigration viewMig = new ViewMigration();
        for (final ViewMigration view : views) {
            viewMig.edgesPostAggregationFilter.putAll(view.edgesPostAggregationFilter);
            viewMig.entitiesPostAggregationFilter.putAll(view.entitiesPostAggregationFilter);
            viewMig.edgesPostTransformFilter.putAll(view.edgesPostTransformFilter);
            viewMig.entitiesPostTransformFilter.putAll(view.entitiesPostTransformFilter);
            viewMig.edgesTransformer.putAll(view.edgesTransformer);
            viewMig.entitiesTransformer.putAll(view.entitiesTransformer);
        }

        final List<Operation> updatedOps = new ArrayList<>();
        if (aggregateAfter) {
            final Aggregate aggregate = new Aggregate();
            updatedOps.add(aggregate);
        }

        if (!viewMig.entitiesPostAggregationFilter.isEmpty()
                || !viewMig.edgesPostAggregationFilter.isEmpty()) {
            final Filter postAggregationFilter = new Filter.Builder()
                    .entities(viewMig.entitiesPostAggregationFilter)
                    .edges(viewMig.edgesPostAggregationFilter)
                    .build();
            updatedOps.add(postAggregationFilter);
        }

        if (!viewMig.entitiesTransformer.isEmpty()
                || !viewMig.edgesTransformer.isEmpty()) {
            final Transform transformFunction = new Transform.Builder()
                    .entities(viewMig.entitiesTransformer)
                    .edges(viewMig.edgesTransformer)
                    .build();
            updatedOps.add(transformFunction);
        }

        if (!viewMig.edgesPostTransformFilter.isEmpty()
                || !viewMig.entitiesPostTransformFilter.isEmpty()) {
            final Filter postTransformFilter = new Filter.Builder()
                    .entities(viewMig.entitiesPostTransformFilter)
                    .edges(viewMig.edgesPostTransformFilter)
                    .build();
            updatedOps.add(postTransformFilter);
        }

        return updatedOps;
    }

    public static ElementFilter createTransformAndFilter(final ElementTransformer transform, final ElementFilter filter) {
        return new ElementFilter.Builder()
                .select(ElementTuple.ELEMENT)
                .execute(new TransformAndFilter(transform, filter))
                .build();
    }

    public void update(final MigrateElement.ElementType migrationElementType, final String migrationGroup, final ViewElementDefinition elementDefinition) {
        if (isNotEmpty(elementDefinition.getPostAggregationFilterFunctions())) {
            updatePostAggregationFilters(migrationElementType, migrationGroup, elementDefinition.getPostAggregationFilter());
            this.viewElementDefinition.setPostAggregationFilter(null);
        }
        if (isNotEmpty(elementDefinition.getTransformFunctions())) {
            updateTransformer(migrationElementType, migrationGroup, elementDefinition.getTransformer());
            this.viewElementDefinition.setTransformer(null);
        }
        if (isNotEmpty(elementDefinition.getPostTransformFilterFunctions())) {
            updatePostTransformFilters(migrationElementType, migrationGroup, elementDefinition.getPostTransformFilter());
            this.viewElementDefinition.setPostTransformFilter(null);
        }
    }

    public void updatePostTransformFilters(final MigrateElement.ElementType migrationElementType, final String migrationGroup, final ElementFilter filter) {
        if (null != filter && isNotEmpty(filter.getComponents())) {
            if (migrationElementType.equals(MigrateElement.ElementType.ENTITY)) {
                entitiesPostTransformFilter.put(migrationGroup, filter);
            } else if (migrationElementType.equals(MigrateElement.ElementType.EDGE)) {
                edgesPostTransformFilter.put(migrationGroup, filter);
            }
        }
    }

    public void updatePostTransformFilters(final MigrateElement.ElementType migrationElementType, final String migrationGroup, final ElementTransformer transformer, final ElementFilter filter) {
        if (null != filter && isNotEmpty(filter.getComponents())) {
            updatePostTransformFilters(migrationElementType, migrationGroup, createTransformAndFilter(transformer, filter));
        }
    }

    public void updatePostAggregationFilters(final MigrateElement.ElementType migrationElementType, final String migrationGroup, final ElementFilter filter) {
        if (null != filter && isNotEmpty(filter.getComponents())) {
            if (migrationElementType.equals(MigrateElement.ElementType.ENTITY)) {
                entitiesPostAggregationFilter.put(migrationGroup, filter);
            } else if (migrationElementType.equals(MigrateElement.ElementType.EDGE)) {
                edgesPostAggregationFilter.put(migrationGroup, filter);
            }
        }
    }

    public void updatePostAggregationFilters(final MigrateElement.ElementType migrationElementType, final String migrationGroup, final ElementTransformer transformer, final ElementFilter filter) {
        if (null != filter && isNotEmpty(filter.getComponents())) {
            updatePostAggregationFilters(migrationElementType, migrationGroup, createTransformAndFilter(transformer, filter));
        }
    }

    public void updateTransformer(final MigrateElement.ElementType migrationElementType, final String migrationGroup, final ElementTransformer transformer) {
        if (null != transformer && isNotEmpty(transformer.getComponents())) {
            if (migrationElementType.equals(MigrateElement.ElementType.ENTITY)) {
                entitiesTransformer.put(migrationGroup, transformer);
            } else if (migrationElementType.equals(MigrateElement.ElementType.EDGE)) {
                edgesTransformer.put(migrationGroup, transformer);
            }
        }
    }

    public ViewElementDefinition getViewElementDefinition() {
        return viewElementDefinition;
    }

    public void setViewElementDefinition(final ViewElementDefinition viewElementDefinition) {
        this.viewElementDefinition = viewElementDefinition;
    }
}
