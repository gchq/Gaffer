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
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
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
 * Applies the migrations required for a {@link uk.gov.gchq.gaffer.data.elementdefinition.view.View}.
 */
public class ViewMigration {
    private final Map<String, ElementFilter> entitiesPostAggregationFilter = new HashMap<>();
    private final Map<String, ElementFilter> edgesPostAggregationFilter = new HashMap<>();
    private final Map<String, ElementTransformer> entitiesTransformer = new HashMap<>();
    private final Map<String, ElementTransformer> edgesTransformer = new HashMap<>();
    private final Map<String, ElementFilter> entitiesPostTransformFilter = new HashMap<>();
    private final Map<String, ElementFilter> edgesPostTransformFilter = new HashMap<>();
    private final boolean aggregateAfter;

    private ViewElementDefinition.Builder elementDefBuilder = new ViewElementDefinition.Builder();

    public ViewMigration(final boolean aggregateAfter) {
        this.aggregateAfter = aggregateAfter;
    }

    public static List<Operation> createMigrationOps(final boolean aggregateAfter,
                                                     final Iterable<ViewMigration> views1,
                                                     final Iterable<ViewMigration> views2) {
        return createMigrationOps(aggregateAfter, IterableUtil.concat(Arrays.asList(views1, views2)));
    }

    public static List<Operation> createMigrationOps(final boolean aggregateAfter,
                                                     final Iterable<ViewMigration> views) {
        final ViewMigration viewMig = new ViewMigration(aggregateAfter);
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
        }

        return updatedOps;
    }

    public ViewMigration update(final MigrateElement migration,
                                final String group,
                                final ViewElementDefinition elementDefinition) {
        this.elementDefBuilder.merge(elementDefinition);
        if (isNotEmpty(elementDefinition.getPostAggregationFilterFunctions())) {
            updatePostAggregationFilters(migration, group, elementDefinition);
            this.elementDefBuilder.clearPostAggregationFilter();
        }
        if (isNotEmpty(elementDefinition.getTransformFunctions())) {
            updateTransformer(migration.getElementType(), group, elementDefinition.getTransformer());
            this.elementDefBuilder.clearTransform();
        }
        if (isNotEmpty(elementDefinition.getPostTransformFilterFunctions())) {
            updatePostTransformFilters(migration, group, elementDefinition.getPostTransformFilter());
            this.elementDefBuilder.clearPostTransformFilter();
        }
        return this;
    }

    public ViewMigration prepareBuilder(final ViewElementDefinition elementDef) {
        elementDefBuilder.merge(elementDef).clearFunctions();
        return this;
    }

    public ViewMigration updatePreAggregationFilters(final ElementTransformer transformer,
                                                     final ViewElementDefinition elementDef) {
        return updatePreAggregationFilters(transformer, elementDef.getPreAggregationFilter());
    }

    public ViewMigration updatePreAggregationFilters(final ElementTransformer transformer,
                                                     final ElementFilter filter) {
        if (null != filter && isNotEmpty(filter.getComponents())) {
            elementDefBuilder.preAggregationFilter(createTransformAndFilter(transformer, filter));
        }
        return this;
    }

    public ViewMigration updatePreAggregationFilters(final ViewElementDefinition elementDef) {
        return updatePreAggregationFilters(elementDef.getPreAggregationFilter());
    }

    public ViewMigration updatePreAggregationFilters(final ElementFilter filter) {
        elementDefBuilder.preAggregationFilter(filter);
        return this;
    }

    public ViewMigration updateAggregator(final ViewElementDefinition elementDef) {
        return updateAggregator(elementDef.getAggregator());
    }

    public ViewMigration updateAggregator(final ElementAggregator aggregator) {
        elementDefBuilder.aggregator(aggregator);
        return this;
    }

    public ViewMigration updatePostAggregationFilters(final MigrateElement migration,
                                                      final String group,
                                                      final ViewElementDefinition elementDef) {
        updatePostAggregationFilters(migration, group, elementDef.getPostAggregationFilter());
        return this;
    }

    public ViewMigration updatePostAggregationFilters(final MigrateElement migration,
                                                      final String group,
                                                      final ElementTransformer transformer,
                                                      final ViewElementDefinition elementDef) {
        return updatePostAggregationFilters(migration, group, transformer, elementDef.getPostAggregationFilter());
    }

    public ViewMigration updatePostAggregationFilters(final MigrateElement migration,
                                                      final String group,
                                                      final ElementTransformer transformer,
                                                      final ElementFilter filter) {
        if (null != filter && isNotEmpty(filter.getComponents())) {
            updatePostAggregationFilters(migration, group, createTransformAndFilter(transformer, filter));
        }
        return this;
    }

    public ViewMigration updatePostAggregationFilters(final MigrateElement migration,
                                                      final String group,
                                                      final ElementFilter filter) {
        if (null != filter && isNotEmpty(filter.getComponents())) {
            if (aggregateAfter) {
                if (migration.getElementType().equals(MigrateElement.ElementType.ENTITY)) {
                    entitiesPostAggregationFilter.put(group, filter);
                } else if (migration.getElementType().equals(MigrateElement.ElementType.EDGE)) {
                    edgesPostAggregationFilter.put(group, filter);
                }
            } else {
                elementDefBuilder.postAggregationFilterFunctions(filter.getComponents());
            }
        }
        return this;
    }

    public ViewMigration updateTransformer(final MigrateElement migration,
                                           final String group,
                                           final ElementTransformer migrationTransform,
                                           final ViewElementDefinition elementDef) {
        return updateTransformer(migration, group, migrationTransform, elementDef.getTransformer());
    }

    public ViewMigration updateTransformer(final MigrateElement migration,
                                           final String group,
                                           final ElementTransformer migrationTransform,
                                           final ElementTransformer userTransform) {
        elementDefBuilder.addTransformFunctions(migrationTransform.getComponents());
        updateTransformer(migration.getElementType(), group, userTransform);
        return this;
    }

    public ViewMigration updateTransformer(final MigrateElement.ElementType migrationElementType,
                                           final String group,
                                           final ElementTransformer transformer) {
        if (null != transformer && isNotEmpty(transformer.getComponents())) {
            if (aggregateAfter) {
                if (migrationElementType.equals(MigrateElement.ElementType.ENTITY)) {
                    entitiesTransformer.put(group, transformer);
                } else if (migrationElementType.equals(MigrateElement.ElementType.EDGE)) {
                    edgesTransformer.put(group, transformer);
                }
            } else {
                elementDefBuilder.addTransformFunctions(transformer.getComponents());
            }
        }
        return this;
    }

    public ViewMigration updatePostTransformFilters(final MigrateElement migration,
                                                    final String group,
                                                    final ElementTransformer transformer,
                                                    final ViewElementDefinition elementDef) {
        updatePostTransformFilters(migration, group, transformer, elementDef.getPostTransformFilter());
        return this;
    }

    public ViewMigration updatePostTransformFilters(final MigrateElement migration,
                                                    final String group,
                                                    final ElementTransformer transformer,
                                                    final ElementFilter filter) {
        updatePostTransformFilters(migration, group, createTransformAndFilter(transformer, filter));
        return this;
    }


    public ViewMigration updatePostTransformFilters(final MigrateElement migration,
                                                    final String group,
                                                    final ViewElementDefinition elementDef) {
        return updatePostTransformFilters(migration, group, elementDef.getPostTransformFilter());
    }

    public ViewMigration updatePostTransformFilters(final MigrateElement migration,
                                                    final String group,
                                                    final ElementFilter filter) {
        if (null != filter && isNotEmpty(filter.getComponents())) {
            if (aggregateAfter) {
                if (migration.getElementType().equals(MigrateElement.ElementType.ENTITY)) {
                    entitiesPostTransformFilter.put(group, filter);
                } else if (migration.getElementType().equals(MigrateElement.ElementType.EDGE)) {
                    edgesPostTransformFilter.put(group, filter);
                }
            } else {
                elementDefBuilder.postTransformFilterFunctions(filter.getComponents());
            }
        }
        return this;
    }

    public ViewElementDefinition buildViewElementDefinition() {
        return elementDefBuilder.build();
    }

    private ElementFilter createTransformAndFilter(final ElementTransformer transform,
                                                   final ElementFilter filter) {
        return new ElementFilter.Builder()
                .select(ElementTuple.ELEMENT)
                .execute(new TransformAndFilter(transform, filter))
                .build();
    }
}
