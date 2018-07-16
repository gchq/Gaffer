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

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.data.element.ElementTuple;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition.Builder;
import uk.gov.gchq.gaffer.graph.hook.GraphHook;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.graph.OperationView;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.schema.ViewValidator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.commons.collections.CollectionUtils.isNotEmpty;

/**
 * A {@link SchemaMigration} {@link GraphHook} allows an admin to set migration mappings
 * that are then applied on any {@link Operation} with output and an {@link OperationView}.
 * <p>
 * To make use of this {@link SchemaMigration} the implemented {@link uk.gov.gchq.gaffer.store.Store} must have the Transform trait.
 */
@JsonPropertyOrder(value = {"entities", "edges", "transformToNew"}, alphabetic = true)
public class SchemaMigration implements GraphHook {
    public static final MigrationOutputType DEFAULT_OUTPUT_TYPE = MigrationOutputType.OLD;
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaMigration.class);

    public boolean aggregateAfter = false;
    private List<MigrateElement> entities = new ArrayList<>();
    private List<MigrateElement> edges = new ArrayList<>();

    private MigrationOutputType outputType = DEFAULT_OUTPUT_TYPE;

    public enum MigrationOutputType {
        NEW, OLD
    }

    @Override
    public void preExecute(final OperationChain<?> opChain, final Context context) {
        if (!edges.isEmpty() || !entities.isEmpty()) {
            final List<Operation> updatedOps = new ArrayList<>();
            for (final Operation op : new ArrayList<>(opChain.flatten())) {
                updatedOps.add(op);
                if (OperationView.hasView(op)) {
                    updatedOps.addAll(migrateOperation(op));
                }
            }
            opChain.updateOperations(updatedOps);
        }
    }

    private List<Operation> migrateOperation(final Operation op) {
        final OperationView opView = OperationView.class.cast(op);
        final Map<String, ViewConfig> migratedEntities = migrateElements(entities, opView.getView()::getEntity);
        final Map<String, ViewConfig> migratedEdges = migrateElements(edges, opView.getView()::getEdge);
        final ViewConfig viewConfig = ViewConfig.merge(aggregateAfter, migratedEdges.values(), migratedEntities.values());
        updateView(opView, migratedEdges, migratedEntities);
        opView.getView().addConfig(ViewValidator.SKIP_VIEW_VALIDATION, Boolean.toString(true));

        return viewConfig.createMigrationOps(viewConfig);
    }

    private void updateView(final OperationView opView, final Map<String, ViewConfig> migratedEdges, final Map<String, ViewConfig> migratedEntities) {
        final View currentView = opView.getView();
        View.Builder viewBuilder = new View.Builder().merge(currentView);
        for (final Map.Entry<String, ViewConfig> entry : migratedEntities.entrySet()) {
            viewBuilder.entity(entry.getKey(), entry.getValue().getViewElementDefinition());
        }
        for (final Map.Entry<String, ViewConfig> entry : migratedEdges.entrySet()) {
            viewBuilder.edge(entry.getKey(), entry.getValue().getViewElementDefinition());
        }
        opView.setView(viewBuilder.build());
        LOGGER.debug("Migrated view: {}", opView.getView());
    }

    private Map<String, ViewConfig> migrateElements(
            final List<MigrateElement> migrations,
            final Function<String, ViewElementDefinition> currentElementProvider) {

        final Map<String, ViewConfig> newElementDefs = new HashMap<>();
        for (final MigrateElement migration : migrations) {
            applyMigration(currentElementProvider, newElementDefs, migration);
        }
        return newElementDefs;
    }

    private void applyMigration(
            final Function<String, ViewElementDefinition> currentElementProvider,
            final Map<String, ViewConfig> newElementDefs,
            final MigrateElement migration) {

        final ViewElementDefinition originalOldElement = currentElementProvider.apply(migration.getOldGroup());
        final ViewElementDefinition originalNewElement = currentElementProvider.apply(migration.getNewGroup());
        final boolean queriedForOld = null != originalOldElement;
        final boolean queriedForNew = null != originalNewElement;

        if (queriedForOld || queriedForNew) {
            final ViewConfig oldElement;
            final ViewConfig newElement;

            if (queriedForOld && queriedForNew) {
                // Queried for old and new
                oldElement = createViewConfigOldFromOld(migration, originalOldElement);
                newElement = createViewConfigNewFromNew(migration, originalNewElement);
            } else if (queriedForOld) {
                // Queried for old
                oldElement = createViewConfigOldFromOld(migration, originalOldElement);
                newElement = createViewConfigNewFromOld(migration, originalOldElement);
            } else {
                // Queried for new
                oldElement = createViewConfigOldFromNew(migration, originalNewElement);
                newElement = createViewConfigNewFromNew(migration, originalNewElement);
            }

            newElementDefs.put(migration.getOldGroup(), oldElement);
            newElementDefs.put(migration.getNewGroup(), newElement);
        }
    }

    private ViewConfig createViewConfigNewFromNew(final MigrateElement migration, final ViewElementDefinition newElement) {
        final ViewConfig viewConfig = new ViewConfig(aggregateAfter);
        if (MigrationOutputType.NEW == outputType || migration.getToOldTransform().getComponents().isEmpty()) {
            viewConfig.setViewElementDefinition(newElement);
            viewConfig.update(migration.getElementType(), migration.getNewGroup(), newElement);
            return viewConfig;
        }

        final ViewElementDefinition.Builder viewBuilder = new ViewElementDefinition.Builder()
                .merge(newElement)
                .addTransformFunctions(migration.getToOldTransform().getComponents());

        setPreAggregationFilters(viewBuilder, newElement.getPreAggregationFilter());
        setPostAggregationFilters(migration, viewConfig, viewBuilder, migration.getNewGroup(), newElement.getPostAggregationFilter());
        setTransformer(migration, viewConfig, migration.getNewGroup(), newElement.getTransformer());
        setPostTransformFilters(migration, viewConfig, viewBuilder, migration.getNewGroup(), migration.getToNewTransform(), newElement.getPostTransformFilter());

        updateElementDefinition(viewBuilder, newElement);
        viewConfig.setViewElementDefinition(viewBuilder.build());

        return viewConfig;
    }

    private ViewConfig createViewConfigNewFromOld(final MigrateElement migration, final ViewElementDefinition oldElement) {
        final ViewConfig viewConfig = new ViewConfig(aggregateAfter);
        if (migration.getToNewTransform().getComponents().isEmpty()) {
            viewConfig.setViewElementDefinition(oldElement);
            viewConfig.update(migration.getElementType(), migration.getNewGroup(), oldElement);
            return viewConfig;
        }

        final ViewElementDefinition.Builder viewBuilder = new ViewElementDefinition.Builder();

        setPreAggregationFilters(viewBuilder, migration.getToOldTransform(), oldElement.getPreAggregationFilter());
        setPostAggregationFilters(migration, viewConfig, migration.getNewGroup(), migration.getToOldTransform(), oldElement.getPostTransformFilter());
        if (MigrationOutputType.NEW == outputType) {
            viewBuilder.addTransformFunctions(migration.getToNewTransform().getComponents());
            setTransformer(migration, viewConfig, migration.getNewGroup(), oldElement.getTransformer());
            setPostTransformFilters(migration, viewConfig, viewBuilder, migration.getNewGroup(), migration.getToOldTransform(), oldElement.getPostTransformFilter());
        } else {
            viewBuilder.addTransformFunctions(migration.getToOldTransform().getComponents());
            setTransformer(migration, viewConfig, migration.getOldGroup(), oldElement.getTransformer());
            setPostTransformFilters(migration, viewConfig, migration.getOldGroup(), oldElement.getPostTransformFilter());
        }

        updateElementDefinition(viewBuilder, oldElement);
        viewConfig.setViewElementDefinition(viewBuilder.build());

        return viewConfig;
    }

    private ViewConfig createViewConfigOldFromOld(final MigrateElement migration, final ViewElementDefinition oldElement) {
        final ViewConfig viewConfig = new ViewConfig(aggregateAfter);
        if (MigrationOutputType.OLD == outputType || migration.getToNewTransform().getComponents().isEmpty()) {
            viewConfig.setViewElementDefinition(oldElement);
            viewConfig.update(migration.getElementType(), migration.getOldGroup(), oldElement);
            return viewConfig;
        }

        final ViewElementDefinition.Builder viewBuilder = new ViewElementDefinition.Builder()
                .merge(oldElement)
                .transformer(migration.getToNewTransform());

        setPreAggregationFilters(viewBuilder, oldElement.getPreAggregationFilter());
        setPostAggregationFilters(migration, viewConfig, viewBuilder, migration.getOldGroup(), oldElement.getPostAggregationFilter());
        setTransformer(migration, viewConfig, migration.getOldGroup(), oldElement.getTransformer());
        setPostTransformFilters(migration, viewConfig, viewBuilder, migration.getOldGroup(), migration.getToOldTransform(), oldElement.getPostTransformFilter());

        updateElementDefinition(viewBuilder, oldElement);
        viewConfig.setViewElementDefinition(viewBuilder.build());

        return viewConfig;
    }

    private ViewConfig createViewConfigOldFromNew(final MigrateElement migration, final ViewElementDefinition newElement) {
        final ViewConfig viewConfig = new ViewConfig(aggregateAfter);
        if (migration.getToOldTransform().getComponents().isEmpty()) {
            viewConfig.setViewElementDefinition(newElement);
            viewConfig.update(migration.getElementType(), migration.getOldGroup(), newElement);
            return viewConfig;
        }

        final ViewElementDefinition.Builder viewBuilder = new ViewElementDefinition.Builder();

        setPreAggregationFilters(viewBuilder, migration.getToNewTransform(), newElement.getPreAggregationFilter());
        setPostAggregationFilters(migration, viewConfig, migration.getOldGroup(), migration.getToNewTransform(), newElement.getPostAggregationFilter());
        if (MigrationOutputType.OLD == outputType) {
            viewBuilder.addTransformFunctions(migration.getToOldTransform().getComponents());
            setTransformer(migration, viewConfig, migration.getOldGroup(), newElement.getTransformer());
            setPostTransformFilters(migration, viewConfig, migration.getOldGroup(), createTransformAndFilter(migration.getToNewTransform(), newElement.getPostTransformFilter()));
        } else {
            viewBuilder.addTransformFunctions(migration.getToNewTransform().getComponents());
            setTransformer(migration, viewConfig, migration.getNewGroup(), newElement.getTransformer());
            setPostTransformFilters(migration, viewConfig, migration.getNewGroup(), newElement.getPostTransformFilter());
        }
        updateElementDefinition(viewBuilder, newElement);
        viewConfig.setViewElementDefinition(viewBuilder.build());

        return viewConfig;
    }

    private void setPreAggregationFilters(final Builder viewBuilder,
                                          final ElementTransformer transformer,
                                          final ElementFilter filter) {
        if (null != filter && isNotEmpty(filter.getComponents())) {
            viewBuilder.preAggregationFilter(createTransformAndFilter(transformer, filter));
        }
    }

    private void setPreAggregationFilters(final Builder viewBuilder,
                                          final ElementFilter filter) {
        if (null != filter && isNotEmpty(filter.getComponents())) {
            viewBuilder.clearPreAggregationFilter();
            viewBuilder.preAggregationFilter(filter);
        }
    }

    private void setPostAggregationFilters(final MigrateElement migration,
                                           final ViewConfig viewConfig,
                                           final Builder viewBuilder,
                                           final String group,
                                           final ElementFilter filter) {
        if (null != filter && isNotEmpty(filter.getComponents())) {
            viewBuilder.clearPostAggregationFilter();
            viewConfig.updatePostAggregationFilters(migration.getElementType(), group, filter);
        }
    }

    private void setPostAggregationFilters(final MigrateElement migration,
                                           final ViewConfig viewConfig,
                                           final String newGroup,
                                           final ElementTransformer transformer,
                                           final ElementFilter filter) {
        if (null != filter && isNotEmpty(filter.getComponents())) {
            viewConfig.updatePostAggregationFilters(
                    migration.getElementType(), newGroup,
                    createTransformAndFilter(transformer, filter));
        }
    }

    private void setTransformer(final MigrateElement migration,
                                final ViewConfig viewConfig,
                                final String group,
                                final ElementTransformer transformer) {
        if (null != transformer && isNotEmpty(transformer.getComponents())) {
            viewConfig.updateTransformerMap(migration.getElementType(), group, transformer);
        }
    }

    private void setPostTransformFilters(final MigrateElement migration,
                                         final ViewConfig viewConfig,
                                         final Builder viewBuilder,
                                         final String newGroup,
                                         final ElementTransformer transformer,
                                         final ElementFilter filter) {
        if (null != filter && isNotEmpty(filter.getComponents())) {
            viewBuilder.clearPostTransformFilter();
            viewConfig.updatePostTransformFilters(migration.getElementType(), newGroup, createTransformAndFilter(transformer, filter));
        }
    }

    private void setPostTransformFilters(final MigrateElement migration,
                                         final ViewConfig viewConfig,
                                         final String group,
                                         final ElementFilter filter) {
        if (null != filter && isNotEmpty(filter.getComponents())) {
            viewConfig.updatePostTransformFilters(migration.getElementType(), group, filter);
        }
    }

    private ElementFilter createTransformAndFilter(final ElementTransformer transform, final ElementFilter filter) {
        return new ElementFilter.Builder()
                .select(ElementTuple.ELEMENT)
                .execute(new TransformAndFilter(transform, filter))
                .build();
    }

    private void updateElementDefinition(final ViewElementDefinition.Builder viewBuilder, final ViewElementDefinition elementDefinition) {
        if (null != elementDefinition.getGroupBy()) {
            viewBuilder.groupBy(elementDefinition.getGroupBy().toArray(new String[elementDefinition.getGroupBy().size()]));
        }
        if (null != elementDefinition.getAggregator()) {
            viewBuilder.aggregator(elementDefinition.getAggregator());
        }
        if (null != elementDefinition.getTransientProperties()) {
            viewBuilder.transientProperties(elementDefinition.getTransientPropertyMap());
        }
        if (null != elementDefinition.getProperties()) {
            viewBuilder.properties(elementDefinition.getProperties());
        }
        if (null != elementDefinition.getExcludeProperties()) {
            viewBuilder.excludeProperties(elementDefinition.getExcludeProperties());
        }
    }

    public List<MigrateElement> getEntities() {
        return entities;
    }

    public void setEntities(final List<MigrateElement> entities) {
        this.entities.clear();
        if (null != entities) {
            this.entities.addAll(entities);
        }
    }

    public List<MigrateElement> getEdges() {
        return edges;
    }

    public void setEdges(final List<MigrateElement> edges) {
        this.edges.clear();
        if (null != edges) {
            this.edges.addAll(edges);
        }
    }

    public MigrationOutputType getOutputType() {
        return outputType;
    }

    public void setOutputType(final MigrationOutputType outputType) {
        if (null == outputType) {
            this.outputType = DEFAULT_OUTPUT_TYPE;
        }
        this.outputType = outputType;
    }

    public boolean isAggregateAfter() {
        return aggregateAfter;
    }

    public void setAggregateAfter(final boolean aggregateAfter) {
        this.aggregateAfter = aggregateAfter;
    }
}
