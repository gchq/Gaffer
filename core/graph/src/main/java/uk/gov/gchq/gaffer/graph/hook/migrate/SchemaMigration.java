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

import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
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
    public static final String TRUE = Boolean.toString(true);

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

    private List<Operation> migrateOperation(final Operation op) {
        final OperationView opView = OperationView.class.cast(op);

        final Map<String, ViewMigration> migratedEntities = migrateViewElements(entities, opView.getView()::getEntity);
        final Map<String, ViewMigration> migratedEdges = migrateViewElements(edges, opView.getView()::getEdge);

        final View.Builder viewBuilder = new View.Builder().merge(opView.getView());
        for (final Map.Entry<String, ViewMigration> entry : migratedEntities.entrySet()) {
            viewBuilder.entity(entry.getKey(), entry.getValue().getViewElementDefinition());
        }
        for (final Map.Entry<String, ViewMigration> entry : migratedEdges.entrySet()) {
            viewBuilder.edge(entry.getKey(), entry.getValue().getViewElementDefinition());
        }
        viewBuilder.config(ViewValidator.SKIP_VIEW_VALIDATION, TRUE);

        final View updatedView = viewBuilder.build();
        LOGGER.debug("Migrated view: {}", updatedView);
        opView.setView(updatedView);

        return ViewMigration.createMigrationOps(aggregateAfter, migratedEdges.values(), migratedEntities.values());
    }

    private Map<String, ViewMigration> migrateViewElements(
            final List<MigrateElement> migrations,
            final Function<String, ViewElementDefinition> currentElementSupplier) {
        final Map<String, ViewMigration> newElementDefs = new HashMap<>();
        migrations.forEach(migration -> applyMigration(currentElementSupplier, newElementDefs, migration));
        return newElementDefs;
    }

    private void applyMigration(
            final Function<String, ViewElementDefinition> currentElementSupplier,
            final Map<String, ViewMigration> newElementDefs,
            final MigrateElement migration) {

        final ViewElementDefinition originalOldElement = currentElementSupplier.apply(migration.getOldGroup());
        final ViewElementDefinition originalNewElement = currentElementSupplier.apply(migration.getNewGroup());
        final boolean queriedForOld = null != originalOldElement;
        final boolean queriedForNew = null != originalNewElement;

        if (queriedForOld || queriedForNew) {
            final ViewMigration oldElement;
            final ViewMigration newElement;

            if (queriedForOld && queriedForNew) {
                // Queried for old and new
                oldElement = migrateViewOldFromOld(migration, originalOldElement);
                newElement = migrateViewNewFromNew(migration, originalNewElement);
            } else if (queriedForOld) {
                // Queried for old
                oldElement = migrateViewOldFromOld(migration, originalOldElement);
                newElement = migrateViewNewFromOld(migration, originalOldElement);
            } else {
                // Queried for new
                oldElement = migrateViewOldFromNew(migration, originalNewElement);
                newElement = migrateViewNewFromNew(migration, originalNewElement);
            }

            newElementDefs.put(migration.getOldGroup(), oldElement);
            newElementDefs.put(migration.getNewGroup(), newElement);
        }
    }

    private ViewMigration migrateViewNewFromNew(final MigrateElement migration, final ViewElementDefinition newElement) {
        final ViewMigration viewMig = new ViewMigration();
        if (MigrationOutputType.NEW == outputType || migration.getToOldTransform().getComponents().isEmpty()) {
            viewMig.setViewElementDefinition(newElement);
            viewMig.update(migration.getElementType(), migration.getNewGroup(), newElement);
        } else {
            final ViewElementDefinition.Builder viewBuilder = createViewBuilder(newElement);
            updatePreAggregationFilters(viewBuilder, newElement.getPreAggregationFilter());
            updateAggregator(viewBuilder, newElement.getAggregator());
            updatePostAggregationFilters(migration, viewMig, migration.getNewGroup(), newElement.getPostAggregationFilter());
            updateTransformer(migration, viewMig, viewBuilder, migration.getNewGroup(), migration.getToOldTransform(), newElement.getTransformer());
            updatePostTransformFilters(migration, viewMig, migration.getNewGroup(), migration.getToNewTransform(), newElement.getPostTransformFilter());
            viewMig.setViewElementDefinition(viewBuilder.build());
        }
        return viewMig;
    }

    private ViewMigration migrateViewNewFromOld(final MigrateElement migration, final ViewElementDefinition oldElement) {
        final ViewMigration viewMig = new ViewMigration();
        if (migration.getToNewTransform().getComponents().isEmpty()) {
            viewMig.setViewElementDefinition(oldElement);
            viewMig.update(migration.getElementType(), migration.getNewGroup(), oldElement);
        } else {
            final ViewElementDefinition.Builder viewBuilder = createViewBuilder(oldElement);
            updatePreAggregationFilters(viewBuilder, migration.getToOldTransform(), oldElement.getPreAggregationFilter());
            updateAggregator(viewBuilder, oldElement.getAggregator());
            updatePostAggregationFilters(migration, viewMig, migration.getNewGroup(), migration.getToOldTransform(), oldElement.getPostTransformFilter());
            if (MigrationOutputType.NEW == outputType) {
                updateTransformer(migration, viewMig, viewBuilder, migration.getNewGroup(), migration.getToNewTransform(), oldElement.getTransformer());
                updatePostTransformFilters(migration, viewMig, migration.getNewGroup(), migration.getToOldTransform(), oldElement.getPostTransformFilter());
            } else {
                updateTransformer(migration, viewMig, viewBuilder, migration.getOldGroup(), migration.getToOldTransform(), oldElement.getTransformer());
                updatePostTransformFilters(migration, viewMig, migration.getOldGroup(), oldElement.getPostTransformFilter());
            }
            viewMig.setViewElementDefinition(viewBuilder.build());
        }
        return viewMig;
    }

    private ViewMigration migrateViewOldFromOld(final MigrateElement migration, final ViewElementDefinition oldElement) {
        final ViewMigration viewMig = new ViewMigration();
        if (MigrationOutputType.OLD == outputType || migration.getToNewTransform().getComponents().isEmpty()) {
            viewMig.setViewElementDefinition(oldElement);
            viewMig.update(migration.getElementType(), migration.getOldGroup(), oldElement);
        } else {
            final ViewElementDefinition.Builder viewBuilder = createViewBuilder(oldElement);
            updatePreAggregationFilters(viewBuilder, oldElement.getPreAggregationFilter());
            updateAggregator(viewBuilder, oldElement.getAggregator());
            updatePostAggregationFilters(migration, viewMig, migration.getOldGroup(), oldElement.getPostAggregationFilter());
            updateTransformer(migration, viewMig, viewBuilder, migration.getOldGroup(), migration.getToNewTransform(), oldElement.getTransformer());
            updatePostTransformFilters(migration, viewMig, migration.getOldGroup(), migration.getToOldTransform(), oldElement.getPostTransformFilter());
            viewMig.setViewElementDefinition(viewBuilder.build());
        }
        return viewMig;
    }

    private ViewMigration migrateViewOldFromNew(final MigrateElement migration, final ViewElementDefinition newElement) {
        final ViewMigration viewMig = new ViewMigration();
        if (migration.getToOldTransform().getComponents().isEmpty()) {
            viewMig.setViewElementDefinition(newElement);
            viewMig.update(migration.getElementType(), migration.getOldGroup(), newElement);
        } else {
            final ViewElementDefinition.Builder viewBuilder = createViewBuilder(newElement);
            updatePreAggregationFilters(viewBuilder, migration.getToNewTransform(), newElement.getPreAggregationFilter());
            updateAggregator(viewBuilder, newElement.getAggregator());
            updatePostAggregationFilters(migration, viewMig, migration.getOldGroup(), migration.getToNewTransform(), newElement.getPostAggregationFilter());
            if (MigrationOutputType.OLD == outputType) {
                updateTransformer(migration, viewMig, viewBuilder, migration.getOldGroup(), migration.getToOldTransform(), newElement.getTransformer());
                updatePostTransformFilters(migration, viewMig, migration.getOldGroup(), migration.getToNewTransform(), newElement.getPostTransformFilter());
            } else {
                updateTransformer(migration, viewMig, viewBuilder, migration.getNewGroup(), migration.getToNewTransform(), newElement.getTransformer());
                updatePostTransformFilters(migration, viewMig, migration.getNewGroup(), newElement.getPostTransformFilter());
            }
            viewMig.setViewElementDefinition(viewBuilder.build());
        }
        return viewMig;
    }

    private ViewElementDefinition.Builder createViewBuilder(final ViewElementDefinition elementDef) {
        return new ViewElementDefinition.Builder().merge(elementDef).clearFunctions();
    }

    private void updatePreAggregationFilters(final Builder viewBuilder,
                                             final ElementTransformer transformer,
                                             final ElementFilter filter) {
        viewBuilder.preAggregationFilter(ViewMigration.createTransformAndFilter(transformer, filter));
    }

    private void updatePreAggregationFilters(final Builder viewBuilder,
                                             final ElementFilter filter) {
        viewBuilder.preAggregationFilter(filter);
    }

    private void updateAggregator(final Builder viewBuilder,
                                  final ElementAggregator aggregator) {
        viewBuilder.aggregator(aggregator);
    }

    private void updatePostAggregationFilters(final MigrateElement migration,
                                              final ViewMigration view,
                                              final String group,
                                              final ElementFilter filter) {
        view.updatePostAggregationFilters(migration.getElementType(), group, filter);
    }

    private void updatePostAggregationFilters(final MigrateElement migration,
                                              final ViewMigration view,
                                              final String newGroup,
                                              final ElementTransformer transformer,
                                              final ElementFilter filter) {
        view.updatePostAggregationFilters(migration.getElementType(), newGroup, transformer, filter);
    }

    private void updateTransformer(final MigrateElement migration,
                                   final ViewMigration view,
                                   final Builder viewBuilder,
                                   final String group,
                                   final ElementTransformer migrationTransform,
                                   final ElementTransformer userTransform) {
        viewBuilder.addTransformFunctions(migrationTransform.getComponents());
        view.updateTransformer(migration.getElementType(), group, userTransform);
    }

    private void updatePostTransformFilters(final MigrateElement migration,
                                            final ViewMigration view,
                                            final String newGroup,
                                            final ElementTransformer transformer,
                                            final ElementFilter filter) {
        view.updatePostTransformFilters(migration.getElementType(), newGroup, transformer, filter);
    }

    private void updatePostTransformFilters(final MigrateElement migration,
                                            final ViewMigration view,
                                            final String group,
                                            final ElementFilter filter) {
        view.updatePostTransformFilters(migration.getElementType(), group, filter);
    }
}
