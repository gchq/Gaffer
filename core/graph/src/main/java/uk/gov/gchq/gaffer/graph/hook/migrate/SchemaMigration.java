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
        final Map<String, MigratedView> migratedEntities = migrateElements(entities, opView.getView()::getEntity);
        final Map<String, MigratedView> migratedEdges = migrateElements(edges, opView.getView()::getEdge);
        final MigratedView view = MigratedView.merge(aggregateAfter, migratedEdges.values(), migratedEntities.values());
        updateView(opView, migratedEdges, migratedEntities);
        opView.getView().addConfig(ViewValidator.SKIP_VIEW_VALIDATION, Boolean.toString(true));

        return view.createMigrationOps(view);
    }

    private void updateView(final OperationView opView, final Map<String, MigratedView> migratedEdges, final Map<String, MigratedView> migratedEntities) {
        final View currentView = opView.getView();
        View.Builder viewBuilder = new View.Builder().merge(currentView);
        for (final Map.Entry<String, MigratedView> entry : migratedEntities.entrySet()) {
            viewBuilder.entity(entry.getKey(), entry.getValue().getViewElementDefinition());
        }
        for (final Map.Entry<String, MigratedView> entry : migratedEdges.entrySet()) {
            viewBuilder.edge(entry.getKey(), entry.getValue().getViewElementDefinition());
        }
        opView.setView(viewBuilder.build());
        LOGGER.debug("Migrated view: {}", opView.getView());
    }

    private Map<String, MigratedView> migrateElements(
            final List<MigrateElement> migrations,
            final Function<String, ViewElementDefinition> currentElementProvider) {

        final Map<String, MigratedView> newElementDefs = new HashMap<>();
        for (final MigrateElement migration : migrations) {
            applyMigration(currentElementProvider, newElementDefs, migration);
        }
        return newElementDefs;
    }

    private void applyMigration(
            final Function<String, ViewElementDefinition> currentElementProvider,
            final Map<String, MigratedView> newElementDefs,
            final MigrateElement migration) {

        final ViewElementDefinition originalOldElement = currentElementProvider.apply(migration.getOldGroup());
        final ViewElementDefinition originalNewElement = currentElementProvider.apply(migration.getNewGroup());
        final boolean queriedForOld = null != originalOldElement;
        final boolean queriedForNew = null != originalNewElement;

        if (queriedForOld || queriedForNew) {
            final MigratedView oldElement;
            final MigratedView newElement;

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

    private MigratedView migrateViewNewFromNew(final MigrateElement migration, final ViewElementDefinition newElement) {
        final MigratedView view = new MigratedView(aggregateAfter);
        if (MigrationOutputType.NEW == outputType || migration.getToOldTransform().getComponents().isEmpty()) {
            view.setViewElementDefinition(newElement);
            view.update(migration.getElementType(), migration.getNewGroup(), newElement);
        } else {
            final ViewElementDefinition.Builder viewBuilder = createViewBuilder(newElement);
            setPreAggregationFilters(viewBuilder, newElement.getPreAggregationFilter());
            setAggregator(viewBuilder, newElement.getAggregator());
            setPostAggregationFilters(migration, view, viewBuilder, migration.getNewGroup(), newElement.getPostAggregationFilter());
            setTransformer(migration, view, viewBuilder, migration.getNewGroup(), migration.getToOldTransform(), newElement.getTransformer());
            setPostTransformFilters(migration, view, viewBuilder, migration.getNewGroup(), migration.getToNewTransform(), newElement.getPostTransformFilter());
            view.setViewElementDefinition(viewBuilder.build());
        }

        return view;
    }

    private MigratedView migrateViewNewFromOld(final MigrateElement migration, final ViewElementDefinition oldElement) {
        final MigratedView view = new MigratedView(aggregateAfter);
        if (migration.getToNewTransform().getComponents().isEmpty()) {
            view.setViewElementDefinition(oldElement);
            view.update(migration.getElementType(), migration.getNewGroup(), oldElement);
        } else {
            final ViewElementDefinition.Builder viewBuilder = createViewBuilder(oldElement);
            setPreAggregationFilters(viewBuilder, migration.getToOldTransform(), oldElement.getPreAggregationFilter());
            setAggregator(viewBuilder, oldElement.getAggregator());
            setPostAggregationFilters(migration, view, migration.getNewGroup(), migration.getToOldTransform(), oldElement.getPostTransformFilter());
            if (MigrationOutputType.NEW == outputType) {
                setTransformer(migration, view, viewBuilder, migration.getNewGroup(), migration.getToNewTransform(), oldElement.getTransformer());
                setPostTransformFilters(migration, view, viewBuilder, migration.getNewGroup(), migration.getToOldTransform(), oldElement.getPostTransformFilter());
            } else {
                setTransformer(migration, view, viewBuilder, migration.getOldGroup(), migration.getToOldTransform(), oldElement.getTransformer());
                setPostTransformFilters(migration, view, migration.getOldGroup(), oldElement.getPostTransformFilter());
            }
            view.setViewElementDefinition(viewBuilder.build());
        }

        return view;
    }

    private MigratedView migrateViewOldFromOld(final MigrateElement migration, final ViewElementDefinition oldElement) {
        final MigratedView view = new MigratedView(aggregateAfter);
        if (MigrationOutputType.OLD == outputType || migration.getToNewTransform().getComponents().isEmpty()) {
            view.setViewElementDefinition(oldElement);
            view.update(migration.getElementType(), migration.getOldGroup(), oldElement);
        } else {
            final ViewElementDefinition.Builder viewBuilder = createViewBuilder(oldElement);
            setPreAggregationFilters(viewBuilder, oldElement.getPreAggregationFilter());
            setAggregator(viewBuilder, oldElement.getAggregator());
            setPostAggregationFilters(migration, view, viewBuilder, migration.getOldGroup(), oldElement.getPostAggregationFilter());
            setTransformer(migration, view, viewBuilder, migration.getOldGroup(), migration.getToNewTransform(), oldElement.getTransformer());
            setPostTransformFilters(migration, view, viewBuilder, migration.getOldGroup(), migration.getToOldTransform(), oldElement.getPostTransformFilter());
            view.setViewElementDefinition(viewBuilder.build());
        }
        return view;
    }

    private MigratedView migrateViewOldFromNew(final MigrateElement migration, final ViewElementDefinition newElement) {
        final MigratedView view = new MigratedView(aggregateAfter);
        if (migration.getToOldTransform().getComponents().isEmpty()) {
            view.setViewElementDefinition(newElement);
            view.update(migration.getElementType(), migration.getOldGroup(), newElement);
        } else {
            final ViewElementDefinition.Builder viewBuilder = createViewBuilder(newElement);
            setPreAggregationFilters(viewBuilder, migration.getToNewTransform(), newElement.getPreAggregationFilter());
            setAggregator(viewBuilder, newElement.getAggregator());
            setPostAggregationFilters(migration, view, migration.getOldGroup(), migration.getToNewTransform(), newElement.getPostAggregationFilter());
            if (MigrationOutputType.OLD == outputType) {
                setTransformer(migration, view, viewBuilder, migration.getOldGroup(), migration.getToOldTransform(), newElement.getTransformer());
                setPostTransformFilters(migration, view, migration.getOldGroup(), createTransformAndFilter(migration.getToNewTransform(), newElement.getPostTransformFilter()));
            } else {
                setTransformer(migration, view, viewBuilder, migration.getNewGroup(), migration.getToNewTransform(), newElement.getTransformer());
                setPostTransformFilters(migration, view, migration.getNewGroup(), newElement.getPostTransformFilter());
            }
            view.setViewElementDefinition(viewBuilder.build());
        }

        return view;
    }

    private ViewElementDefinition.Builder createViewBuilder(final ViewElementDefinition elementDef) {
        return new ViewElementDefinition.Builder().merge(elementDef).clearFunctions();
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
            viewBuilder.preAggregationFilter(filter);
        }
    }

    private void setAggregator(final Builder viewBuilder,
                               final ElementAggregator aggregator) {
        if (null != aggregator && isNotEmpty(aggregator.getComponents())) {
            viewBuilder.aggregator(aggregator);
        }
    }

    private void setPostAggregationFilters(final MigrateElement migration,
                                           final MigratedView view,
                                           final Builder viewBuilder,
                                           final String group,
                                           final ElementFilter filter) {
        if (null != filter && isNotEmpty(filter.getComponents())) {
            view.updatePostAggregationFilters(migration.getElementType(), group, filter);
        }
    }

    private void setPostAggregationFilters(final MigrateElement migration,
                                           final MigratedView view,
                                           final String newGroup,
                                           final ElementTransformer transformer,
                                           final ElementFilter filter) {
        if (null != filter && isNotEmpty(filter.getComponents())) {
            view.updatePostAggregationFilters(
                    migration.getElementType(), newGroup,
                    createTransformAndFilter(transformer, filter));
        }
    }

    private void setTransformer(final MigrateElement migration,
                                final MigratedView view,
                                final Builder viewBuilder,
                                final String group,
                                final ElementTransformer migrationTransform,
                                final ElementTransformer userTransform) {
        if (null != migrationTransform && isNotEmpty(migrationTransform.getComponents())) {
            viewBuilder.addTransformFunctions(migrationTransform.getComponents());
        }
        if (null != userTransform && isNotEmpty(userTransform.getComponents())) {
            view.updateTransformerMap(migration.getElementType(), group, userTransform);
        }
    }

    private void setPostTransformFilters(final MigrateElement migration,
                                         final MigratedView view,
                                         final Builder viewBuilder,
                                         final String newGroup,
                                         final ElementTransformer transformer,
                                         final ElementFilter filter) {
        if (null != filter && isNotEmpty(filter.getComponents())) {
            view.updatePostTransformFilters(migration.getElementType(), newGroup, createTransformAndFilter(transformer, filter));
        }
    }

    private void setPostTransformFilters(final MigrateElement migration,
                                         final MigratedView view,
                                         final String group,
                                         final ElementFilter filter) {
        if (null != filter && isNotEmpty(filter.getComponents())) {
            view.updatePostTransformFilters(migration.getElementType(), group, filter);
        }
    }

    private ElementFilter createTransformAndFilter(final ElementTransformer transform, final ElementFilter filter) {
        return new ElementFilter.Builder()
                .select(ElementTuple.ELEMENT)
                .execute(new TransformAndFilter(transform, filter))
                .build();
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
