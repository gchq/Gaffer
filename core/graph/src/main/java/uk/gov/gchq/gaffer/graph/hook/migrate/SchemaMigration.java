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
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.data.element.ElementTuple;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.graph.hook.AddOperationsToChain;
import uk.gov.gchq.gaffer.graph.hook.GraphHook;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.graph.OperationView;
import uk.gov.gchq.gaffer.operation.impl.function.Aggregate;
import uk.gov.gchq.gaffer.store.Context;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@JsonPropertyOrder(value = {"entities", "edges", "transformToNew"}, alphabetic = true)
public class SchemaMigration implements GraphHook {
    public static final MigrationOutputType DEFAULT_OUTPUT_TYPE = MigrationOutputType.OLD;
    public boolean aggregateAfter = false;

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaMigration.class);

    private List<MigrateElement> entities = new ArrayList<>();
    private List<MigrateElement> edges = new ArrayList<>();

    private MigrationOutputType outputType = DEFAULT_OUTPUT_TYPE;

    public enum MigrationOutputType {
        NEW, OLD
    }

    @Override
    public void preExecute(final OperationChain<?> opChain, final Context context) {
        if (!edges.isEmpty() || !entities.isEmpty()) {
            opChain.flatten()
                    .stream()
                    .filter(OperationView::hasView)
                    .map(OperationView.class::cast)
                    .forEach(this::updateView);

            if (aggregateAfter) {
                AddOperationsToChain addOpsHook = new AddOperationsToChain();

                List<? extends Operation> operationsWithViews = opChain.flatten()
                        .stream()
                        .filter(OperationView::hasView)
                        .collect(Collectors.toList());

                final java.util.Map<String, List<Operation>> afterOpsMap = new HashMap<>();

                for (Operation o : operationsWithViews) {
                    afterOpsMap.put(o.getClass().getName(), Lists.newArrayList(new Aggregate()));
                }

                addOpsHook.setAfter(afterOpsMap);

                addOpsHook.preExecute(opChain, context);
            }
        }
    }

    private void updateView(final OperationView op) {
        final View currentView = op.getView();
        op.setView(new View.Builder()
                .merge(currentView)
                .addEntities(getMigratedElements(entities, currentView::getEntity))
                .addEdges(getMigratedElements(edges, currentView::getEdge))
                .build());
        LOGGER.debug("Migrated view: {}", op.getView());
    }

    private Map<String, ViewElementDefinition> getMigratedElements(
            final List<MigrateElement> migrations,
            final Function<String, ViewElementDefinition> currentElementProvider) {

        final Map<String, ViewElementDefinition> newElementDefs = new HashMap<>();
        for (final MigrateElement migration : migrations) {
            createNewElementDefinitions(currentElementProvider, newElementDefs, migration);
        }
        return newElementDefs;
    }

    private void createNewElementDefinitions(
            final Function<String, ViewElementDefinition> currentElementProvider,
            final Map<String, ViewElementDefinition> newElementDefs,
            final MigrateElement migration) {

        final ViewElementDefinition originalOldElement = currentElementProvider.apply(migration.getOldGroup());
        final ViewElementDefinition originalNewElement = currentElementProvider.apply(migration.getNewGroup());
        final boolean queriedForOld = null != originalOldElement;
        final boolean queriedForNew = null != originalNewElement;

        if (queriedForOld || queriedForNew) {
            final ViewElementDefinition oldElement;
            final ViewElementDefinition newElement;

            if (queriedForOld && queriedForNew) {
                // Queried for old and new
                oldElement = createOldElementDefFromOld(migration, originalOldElement);
                newElement = createNewElementDefFromNew(migration, originalNewElement);
            } else if (queriedForOld) {
                // Queried for old
                oldElement = createOldElementDefFromOld(migration, originalOldElement);
                newElement = createNewElementDefFromOld(migration, originalOldElement);
            } else {
                // Queried for new
                oldElement = createOldElementDefFromNew(migration, originalNewElement);
                newElement = createNewElementDefFromNew(migration, originalNewElement);
            }

            newElementDefs.put(migration.getOldGroup(), oldElement);
            newElementDefs.put(migration.getNewGroup(), newElement);
        }
    }

    private ViewElementDefinition createNewElementDefFromOld(final MigrateElement migration, final ViewElementDefinition oldElement) {
        if (migration.getToNew().isEmpty()) {
            return oldElement;
        }

        ViewElementDefinition.Builder viewBuilder = new ViewElementDefinition.Builder();

        if (CollectionUtils.isNotEmpty(oldElement.getPreAggregationFilterFunctions())) {
            viewBuilder = viewBuilder.preAggregationFilter(new ElementFilter.Builder()
                    .select(ElementTuple.ELEMENT)
                    .execute(new TransformAndFilter(migration.getToOldTransform(), oldElement.getPreAggregationFilter()))
                    .build());
        }

        if (CollectionUtils.isNotEmpty(oldElement.getPostAggregationFilterFunctions())) {
            viewBuilder = viewBuilder.postAggregationFilter(new ElementFilter.Builder()
                    .select(ElementTuple.ELEMENT)
                    .execute(new TransformAndFilter(migration.getToOldTransform(), oldElement.getPostAggregationFilter()))
                    .build());
        }

        if (MigrationOutputType.NEW == outputType) {
            if (CollectionUtils.isNotEmpty(oldElement.getTransformFunctions())) {
                viewBuilder = viewBuilder.addTransformFunctions(migration.getToOld());
                viewBuilder.addTransformFunctions(oldElement.getTransformFunctions());
                viewBuilder.addTransformFunctions(migration.getToNew());
            }
        } else {
            viewBuilder = viewBuilder.addTransformFunctions(migration.getToOld());
            if (CollectionUtils.isNotEmpty(oldElement.getTransformFunctions())) {
                viewBuilder.addTransformFunctions(oldElement.getTransformFunctions());
            }
        }

        if (CollectionUtils.isNotEmpty(oldElement.getPostTransformFilterFunctions())) {
            if (MigrationOutputType.NEW == outputType) {
                viewBuilder = viewBuilder.postTransformFilter(new ElementFilter.Builder()
                        .select(ElementTuple.ELEMENT)
                        .execute(new TransformAndFilter(migration.getToOldTransform(), oldElement.getPostTransformFilter()))
                        .build());
            } else {
                viewBuilder.postTransformFilter(oldElement.getPostTransformFilter());
            }
        }

        if (null != oldElement.getGroupBy()) {
            viewBuilder.groupBy(oldElement.getGroupBy().toArray(new String[oldElement.getGroupBy().size()]));
        }
        if (null != oldElement.getAggregator()) {
            viewBuilder.aggregator(oldElement.getAggregator());
        }
        if (null != oldElement.getTransientProperties()) {
            viewBuilder.transientProperties(oldElement.getTransientPropertyMap());
        }
        if (null != oldElement.getProperties()) {
            viewBuilder.properties(oldElement.getProperties());
        }
        if (null != oldElement.getExcludeProperties()) {
            viewBuilder.excludeProperties(oldElement.getExcludeProperties());
        }

        return viewBuilder.build();
    }

    private ViewElementDefinition createOldElementDefFromOld(final MigrateElement migration, final ViewElementDefinition oldElement) {
        if (MigrationOutputType.OLD == outputType || migration.getToNew().isEmpty()) {
            return oldElement;
        }

        ViewElementDefinition.Builder viewBuilder = new ViewElementDefinition.Builder()
                .merge(oldElement)
                .transformFunctions(migration.getToNew());
        if (CollectionUtils.isNotEmpty(oldElement.getPostTransformFilterFunctions())) {
            viewBuilder = viewBuilder.postTransformFilter(new ElementFilter.Builder()
                    .select(ElementTuple.ELEMENT)
                    .execute(new TransformAndFilter(migration.getToOldTransform(), oldElement.getPostTransformFilter()))
                    .build());
        }

        if (null != oldElement.getGroupBy()) {
            viewBuilder.groupBy(oldElement.getGroupBy().toArray(new String[oldElement.getGroupBy().size()]));
        }
        if (null != oldElement.getAggregator()) {
            viewBuilder.aggregator(oldElement.getAggregator());
        }
        if (null != oldElement.getTransientProperties()) {
            viewBuilder.transientProperties(oldElement.getTransientPropertyMap());
        }
        if (null != oldElement.getProperties()) {
            viewBuilder.properties(oldElement.getProperties());
        }
        if (null != oldElement.getExcludeProperties()) {
            viewBuilder.excludeProperties(oldElement.getExcludeProperties());
        }

        return viewBuilder.build();
    }

    private ViewElementDefinition createOldElementDefFromNew(final MigrateElement migration, final ViewElementDefinition newElement) {
        if (migration.getToOld().isEmpty()) {
            return newElement;
        }

        ViewElementDefinition.Builder viewBuilder = new ViewElementDefinition.Builder();

        if (CollectionUtils.isNotEmpty(newElement.getPreAggregationFilterFunctions())) {
            viewBuilder = viewBuilder.preAggregationFilter(new ElementFilter.Builder()
                    .select(ElementTuple.ELEMENT)
                    .execute(new TransformAndFilter(migration.getToNewTransform(), newElement.getPreAggregationFilter()))
                    .build());
        }

        if (CollectionUtils.isNotEmpty(newElement.getPostAggregationFilterFunctions())) {
            viewBuilder = viewBuilder.postAggregationFilter(new ElementFilter.Builder()
                    .select(ElementTuple.ELEMENT)
                    .execute(new TransformAndFilter(migration.getToNewTransform(), newElement.getPostAggregationFilter()))
                    .build());
        }

        if (MigrationOutputType.NEW == outputType) {
            viewBuilder = viewBuilder.transformer(migration.getToNewTransform());
            viewBuilder = viewBuilder.addTransformFunctions(newElement.getTransformFunctions());
            viewBuilder = viewBuilder.postTransformFilter(newElement.getPostTransformFilter());
        } else {
            if (CollectionUtils.isNotEmpty(newElement.getTransformFunctions())) {
                viewBuilder = viewBuilder
                        .addTransformFunctions(migration.getToNew())
                        .addTransformFunctions(newElement.getTransformFunctions())
                        .addTransformFunctions(migration.getToOld());
            }

            if (CollectionUtils.isNotEmpty(newElement.getPostTransformFilterFunctions())) {
                viewBuilder = viewBuilder.postTransformFilter(new ElementFilter.Builder()
                        .select(ElementTuple.ELEMENT)
                        .execute(new TransformAndFilter(migration.getToNewTransform(), newElement.getPostTransformFilter()))
                        .build());
            }
        }

        if (null != newElement.getGroupBy()) {
            viewBuilder.groupBy(newElement.getGroupBy().toArray(new String[newElement.getGroupBy().size()]));
        }
        if (null != newElement.getAggregator()) {
            viewBuilder.aggregator(newElement.getAggregator());
        }
        if (null != newElement.getTransientProperties()) {
            viewBuilder.transientProperties(newElement.getTransientPropertyMap());
        }
        if (null != newElement.getProperties()) {
            viewBuilder.properties(newElement.getProperties());
        }
        if (null != newElement.getExcludeProperties()) {
            viewBuilder.excludeProperties(newElement.getExcludeProperties());
        }

        return viewBuilder.build();
    }

    private ViewElementDefinition createNewElementDefFromNew(final MigrateElement migration, final ViewElementDefinition newElement) {
        if (MigrationOutputType.NEW == outputType || migration.getToOld().isEmpty()) {
            return newElement;
        }

        ViewElementDefinition.Builder viewBuilder = new ViewElementDefinition.Builder()
                .preAggregationFilter(newElement.getPreAggregationFilter())
                .postAggregationFilter(newElement.getPostAggregationFilter());

        if (CollectionUtils.isNotEmpty(newElement.getTransformFunctions())) {
            viewBuilder = viewBuilder.addTransformFunctions(newElement.getTransformFunctions());
        }
        viewBuilder = viewBuilder.addTransformFunctions(migration.getToOld());

        if (CollectionUtils.isNotEmpty(newElement.getPostTransformFilterFunctions())) {
            viewBuilder = viewBuilder.postTransformFilter(new ElementFilter.Builder()
                    .select(ElementTuple.ELEMENT)
                    .execute(new TransformAndFilter(migration.getToNewTransform(), newElement.getPostTransformFilter()))
                    .build());
        }

        if (null != newElement.getGroupBy()) {
            viewBuilder.groupBy(newElement.getGroupBy().toArray(new String[newElement.getGroupBy().size()]));
        }
        if (null != newElement.getAggregator()) {
            viewBuilder.aggregator(newElement.getAggregator());
        }
        if (null != newElement.getTransientProperties()) {
            viewBuilder.transientProperties(newElement.getTransientPropertyMap());
        }
        if (null != newElement.getProperties()) {
            viewBuilder.properties(newElement.getProperties());
        }
        if (null != newElement.getExcludeProperties()) {
            viewBuilder.excludeProperties(newElement.getExcludeProperties());
        }

        return viewBuilder.build();
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
