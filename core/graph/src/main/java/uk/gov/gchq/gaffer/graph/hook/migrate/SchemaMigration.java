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
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.data.element.ElementTuple;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.graph.hook.GraphHook;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.graph.OperationView;
import uk.gov.gchq.gaffer.operation.impl.function.Aggregate;
import uk.gov.gchq.gaffer.operation.impl.function.Filter;
import uk.gov.gchq.gaffer.operation.impl.function.Transform;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.schema.ViewValidator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A {@link SchemaMigration} {@link GraphHook} allows an admin to set migration mappings
 * that are then applied on any {@link Operation} with output and an {@link OperationView}.
 * <p>
 * To make use of this {@link SchemaMigration} the implemented {@link uk.gov.gchq.gaffer.store.Store} must have the Transform trait.
 */
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

    @Override
    public void preExecute(final OperationChain<?> opChain, final Context context) {
        if (!edges.isEmpty() || !entities.isEmpty()) {
            List<? extends Operation> opsWithViewList = opChain.flatten()
                    .stream()
                    .filter(OperationView::hasView)
                    .collect(Collectors.toList());

            for (final Operation op : opsWithViewList) {
                int indexInOpChain = opChain.getOperations().indexOf(op);
                OperationView operationView = OperationView.class.cast(op);

                Map<String, ViewWithTransformationsAndFilters> migratedEntities = getMigratedElements(entities, operationView.getView()::getEntity);
                Map<String, ViewWithTransformationsAndFilters> migratedEdges = getMigratedElements(edges, operationView.getView()::getEdge);

                ViewWithTransformationsAndFilters transformationsAndFilters = getTransformationsAndFilters(migratedEdges, migratedEntities);

                updateView(operationView, migratedEdges, migratedEntities);

                operationView.getView().addConfig(ViewValidator.SKIP_VIEW_VALIDATION, Boolean.toString(true));

                if (aggregateAfter) {
                    if (-1 != indexInOpChain) {
                        opChain.getOperations().add(indexInOpChain + 1, new Aggregate());
                        indexInOpChain += 1;
                    }
                }

                if (!transformationsAndFilters.getEntitiesPostAggregationFilterMap().isEmpty()
                        || !transformationsAndFilters.getEdgesPostAggregationFilterMap().isEmpty()) {
                    Filter postAggregationFilter = new Filter.Builder()
                            .entities(transformationsAndFilters.getEntitiesPostAggregationFilterMap())
                            .edges(transformationsAndFilters.getEdgesPostAggregationFilterMap())
                            .build();
                    if (-1 != indexInOpChain) {
                        opChain.getOperations().add(indexInOpChain + 1, postAggregationFilter);
                        indexInOpChain += 1;
                    }
                }

                if (!transformationsAndFilters.getEntitiesTransformerMap().isEmpty()
                        || !transformationsAndFilters.getEdgesTransformerMap().isEmpty()) {
                    Transform transformFunction = new Transform.Builder()
                            .entities(transformationsAndFilters.getEntitiesTransformerMap())
                            .edges(transformationsAndFilters.getEdgesTransformerMap())
                            .build();
                    if (-1 != indexInOpChain) {
                        opChain.getOperations().add(indexInOpChain + 1, transformFunction);
                        indexInOpChain += 1;
                    }
                }

                if (!transformationsAndFilters.getEdgesPostTransformFilterMap().isEmpty()
                        || !transformationsAndFilters.getEntitiesPostTransformFilterMap().isEmpty()) {
                    Filter postTransformFilter = new Filter.Builder()
                            .entities(transformationsAndFilters.getEntitiesPostTransformFilterMap())
                            .edges(transformationsAndFilters.getEdgesPostTransformFilterMap())
                            .build();
                    if (-1 != indexInOpChain) {
                        opChain.getOperations().add(indexInOpChain + 1, postTransformFilter);
                    }
                }
            }
        }
    }

    private void updateView(final OperationView op, final Map<String, ViewWithTransformationsAndFilters> migratedEdges, final Map<String, ViewWithTransformationsAndFilters> migratedEntities) {
        final View currentView = op.getView();
        View.Builder viewBuilder = new View.Builder().merge(currentView);
        for (Map.Entry<String, ViewWithTransformationsAndFilters> entry : migratedEntities.entrySet()) {
            viewBuilder.entity(entry.getKey(), entry.getValue().getViewElementDefinition());
        }
        for (Map.Entry<String, ViewWithTransformationsAndFilters> entry : migratedEdges.entrySet()) {
            viewBuilder.edge(entry.getKey(), entry.getValue().getViewElementDefinition());
        }
        op.setView(viewBuilder.build());
        LOGGER.debug("Migrated view: {}", op.getView());
    }

    private Map<String, ViewWithTransformationsAndFilters> getMigratedElements(
            final List<MigrateElement> migrations,
            final Function<String, ViewElementDefinition> currentElementProvider) {

        final Map<String, ViewWithTransformationsAndFilters> newElementDefs = new HashMap<>();
        for (final MigrateElement migration : migrations) {
            createSchemaMigrations(currentElementProvider, newElementDefs, migration);
        }
        return newElementDefs;
    }

    private void createSchemaMigrations(
            final Function<String, ViewElementDefinition> currentElementProvider,
            final Map<String, ViewWithTransformationsAndFilters> newElementDefs,
            final MigrateElement migration) {

        final ViewElementDefinition originalOldElement = currentElementProvider.apply(migration.getOldGroup());
        final ViewElementDefinition originalNewElement = currentElementProvider.apply(migration.getNewGroup());
        final boolean queriedForOld = null != originalOldElement;
        final boolean queriedForNew = null != originalNewElement;

        if (queriedForOld || queriedForNew) {
            final ViewWithTransformationsAndFilters oldElement;
            final ViewWithTransformationsAndFilters newElement;

            if (queriedForOld && queriedForNew) {
                // Queried for old and new
                oldElement = createViewWithTransformationsAndFiltersOldFromOld(migration, originalOldElement);
                newElement = createViewWithTransformationsAndFiltersNewFromNew(migration, originalNewElement);
            } else if (queriedForOld) {
                // Queried for old
                oldElement = createViewWithTransformationsAndFiltersOldFromOld(migration, originalOldElement);
                newElement = createViewWithTransformationsAndFiltersNewFromOld(migration, originalOldElement);
            } else {
                // Queried for new
                oldElement = createViewWithTransformAndFiltersOldFromNew(migration, originalNewElement);
                newElement = createViewWithTransformationsAndFiltersNewFromNew(migration, originalNewElement);
            }

            newElementDefs.put(migration.getOldGroup(), oldElement);
            newElementDefs.put(migration.getNewGroup(), newElement);
        }
    }

    private ViewWithTransformationsAndFilters createViewWithTransformationsAndFiltersNewFromNew(final MigrateElement migration, final ViewElementDefinition newElement) {
        ViewWithTransformationsAndFilters viewWithTransformationsAndFilters = new ViewWithTransformationsAndFilters();
        if (MigrationOutputType.NEW == outputType || migration.getToOld().isEmpty()) {
            viewWithTransformationsAndFilters.setViewElementDefinition(newElement);
            updateAllFiltersAndTransforms(migration.getElementType(), migration.getNewGroup(), newElement, viewWithTransformationsAndFilters);
            return viewWithTransformationsAndFilters;
        }

        ViewElementDefinition.Builder viewBuilder = new ViewElementDefinition.Builder()
                .merge(newElement)
                .addTransformFunctions(migration.getToOld());

        if (null != newElement.getPreAggregationFilter()) {
            viewBuilder.clearPreAggregationFilter();
            viewBuilder.preAggregationFilter(newElement.getPreAggregationFilter());
        }
        if (CollectionUtils.isNotEmpty(newElement.getPostAggregationFilterFunctions())) {
            viewBuilder.clearPostAggregationFilter();
            ElementFilter filter = newElement.getPostAggregationFilter();
            updatePostAggregationFilters(migration.getElementType(), migration.getNewGroup(), filter, viewWithTransformationsAndFilters);
        }
        if (CollectionUtils.isNotEmpty(newElement.getTransformFunctions())) {
            ElementTransformer transformer = new ElementTransformer();
            transformer.getComponents().addAll(newElement.getTransformFunctions());
            updateTransformerMap(migration.getElementType(), migration.getNewGroup(), transformer, viewWithTransformationsAndFilters);
        }
        if (CollectionUtils.isNotEmpty(newElement.getPostTransformFilterFunctions())) {
            viewBuilder.clearPostTransformFilter();
            ElementFilter filter = new ElementFilter.Builder()
                    .select(ElementTuple.ELEMENT)
                    .execute(new TransformAndFilter(migration.getToNewTransform(), newElement.getPostTransformFilter()))
                    .build();
            updatePostTransformFilters(migration.getElementType(), migration.getNewGroup(), filter, viewWithTransformationsAndFilters);
        }

        updateElementDefinition(viewBuilder, newElement);
        viewWithTransformationsAndFilters.setViewElementDefinition(viewBuilder.build());

        return viewWithTransformationsAndFilters;
    }

    private ViewWithTransformationsAndFilters createViewWithTransformationsAndFiltersNewFromOld(final MigrateElement migration, final ViewElementDefinition oldElement) {
        final ViewWithTransformationsAndFilters viewWithTransformationsAndFilters = new ViewWithTransformationsAndFilters();
        if (migration.getToNew().isEmpty()) {
            viewWithTransformationsAndFilters.setViewElementDefinition(oldElement);
            updateAllFiltersAndTransforms(migration.getElementType(), migration.getNewGroup(), oldElement, viewWithTransformationsAndFilters);
            return viewWithTransformationsAndFilters;
        }

        ViewElementDefinition.Builder viewBuilder = new ViewElementDefinition.Builder();

        if (CollectionUtils.isNotEmpty(oldElement.getPreAggregationFilterFunctions())) {
            viewBuilder.preAggregationFilter(new ElementFilter.Builder()
                    .select(ElementTuple.ELEMENT)
                    .execute(new TransformAndFilter(migration.getToOldTransform(), oldElement.getPreAggregationFilter()))
                    .build());
        }
        if (CollectionUtils.isNotEmpty(oldElement.getPostAggregationFilterFunctions())) {
            ElementFilter filter = new ElementFilter.Builder()
                    .select(ElementTuple.ELEMENT)
                    .execute(new TransformAndFilter(migration.getToOldTransform(), oldElement.getPostAggregationFilter()))
                    .build();
            updatePostAggregationFilters(migration.getElementType(), migration.getNewGroup(), filter, viewWithTransformationsAndFilters);
        }

        if (MigrationOutputType.NEW == outputType) {
            viewBuilder.addTransformFunctions(migration.getToOld());
            viewBuilder.addTransformFunctions(migration.getToNew());

            if (CollectionUtils.isNotEmpty(oldElement.getTransformFunctions())) {
                ElementTransformer transformer = new ElementTransformer();
                transformer.getComponents().addAll(oldElement.getTransformFunctions());
                updateTransformerMap(migration.getElementType(), migration.getNewGroup(), transformer, viewWithTransformationsAndFilters);
            }
            if (CollectionUtils.isNotEmpty(oldElement.getPostTransformFilterFunctions())) {
                ElementFilter filter = new ElementFilter.Builder()
                        .select(ElementTuple.ELEMENT)
                        .execute(new TransformAndFilter(migration.getToOldTransform(), oldElement.getPostTransformFilter()))
                        .build();
                updatePostTransformFilters(migration.getElementType(), migration.getNewGroup(), filter, viewWithTransformationsAndFilters);
            }
        } else {
            viewBuilder.addTransformFunctions(migration.getToOld());

            if (CollectionUtils.isNotEmpty(oldElement.getTransformFunctions())) {
                ElementTransformer transformer = new ElementTransformer();
                transformer.getComponents().addAll(oldElement.getTransformFunctions());
                updateTransformerMap(migration.getElementType(), migration.getOldGroup(), transformer, viewWithTransformationsAndFilters);
            }
            if (CollectionUtils.isNotEmpty(oldElement.getPostTransformFilterFunctions())) {
                updatePostTransformFilters(migration.getElementType(), migration.getOldGroup(), oldElement.getPostTransformFilter(), viewWithTransformationsAndFilters);
            }
        }

        updateElementDefinition(viewBuilder, oldElement);
        viewWithTransformationsAndFilters.setViewElementDefinition(viewBuilder.build());

        return viewWithTransformationsAndFilters;
    }

    private ViewWithTransformationsAndFilters createViewWithTransformationsAndFiltersOldFromOld(final MigrateElement migration, final ViewElementDefinition oldElement) {
        ViewWithTransformationsAndFilters viewWithTransformationsAndFilters = new ViewWithTransformationsAndFilters();
        if (MigrationOutputType.OLD == outputType || migration.getToNew().isEmpty()) {
            viewWithTransformationsAndFilters.setViewElementDefinition(oldElement);
            updateAllFiltersAndTransforms(migration.getElementType(), migration.getOldGroup(), oldElement, viewWithTransformationsAndFilters);
            return viewWithTransformationsAndFilters;
        }

        ViewElementDefinition.Builder viewBuilder = new ViewElementDefinition.Builder()
                .merge(oldElement)
                .transformFunctions(migration.getToNew());

        if (null != oldElement.getPreAggregationFilter()) {
            viewBuilder.clearPreAggregationFilter();
            viewBuilder.preAggregationFilter(oldElement.getPreAggregationFilter());
        }
        if (CollectionUtils.isNotEmpty(oldElement.getPostAggregationFilterFunctions())) {
            viewBuilder.clearPostAggregationFilter();
            ElementFilter filter = oldElement.getPostAggregationFilter();
            updatePostAggregationFilters(migration.getElementType(), migration.getOldGroup(), filter, viewWithTransformationsAndFilters);
        }
        if (CollectionUtils.isNotEmpty(oldElement.getTransformFunctions())) {
            ElementTransformer transformer = new ElementTransformer();
            transformer.getComponents().addAll(oldElement.getTransformFunctions());
            updateTransformerMap(migration.getElementType(), migration.getOldGroup(), transformer, viewWithTransformationsAndFilters);
        }
        if (CollectionUtils.isNotEmpty(oldElement.getPostTransformFilterFunctions())) {
            viewBuilder.clearPostTransformFilter();
            ElementFilter filter = new ElementFilter.Builder()
                    .select(ElementTuple.ELEMENT)
                    .execute(new TransformAndFilter(migration.getToOldTransform(), oldElement.getPostTransformFilter()))
                    .build();
            updatePostTransformFilters(migration.getElementType(), migration.getOldGroup(), filter, viewWithTransformationsAndFilters);
        }

        updateElementDefinition(viewBuilder, oldElement);
        viewWithTransformationsAndFilters.setViewElementDefinition(viewBuilder.build());

        return viewWithTransformationsAndFilters;
    }

    private ViewWithTransformationsAndFilters createViewWithTransformAndFiltersOldFromNew(final MigrateElement migration, final ViewElementDefinition newElement) {
        final ViewWithTransformationsAndFilters viewWithTransformationsAndFilters = new ViewWithTransformationsAndFilters();
        if (migration.getToOld().isEmpty()) {
            viewWithTransformationsAndFilters.setViewElementDefinition(newElement);
            updateAllFiltersAndTransforms(migration.getElementType(), migration.getOldGroup(), newElement, viewWithTransformationsAndFilters);
            return viewWithTransformationsAndFilters;
        }

        ViewElementDefinition.Builder viewBuilder = new ViewElementDefinition.Builder();

        if (CollectionUtils.isNotEmpty(newElement.getPreAggregationFilterFunctions())) {
            viewBuilder.preAggregationFilter(new ElementFilter.Builder()
                    .select(ElementTuple.ELEMENT)
                    .execute(new TransformAndFilter(migration.getToNewTransform(), newElement.getPreAggregationFilter()))
                    .build());
        }
        if (CollectionUtils.isNotEmpty(newElement.getPostAggregationFilterFunctions())) {
            ElementFilter newFilter = new ElementFilter.Builder()
                    .select(ElementTuple.ELEMENT)
                    .execute(new TransformAndFilter(migration.getToNewTransform(), newElement.getPostAggregationFilter()))
                    .build();
            updatePostAggregationFilters(migration.getElementType(), migration.getOldGroup(), newFilter, viewWithTransformationsAndFilters);
        }

        if (MigrationOutputType.OLD == outputType) {
            viewBuilder.addTransformFunctions(migration.getToNew());
            viewBuilder.addTransformFunctions(migration.getToOld());

            if (CollectionUtils.isNotEmpty(newElement.getTransformFunctions())) {
                ElementTransformer transformer = new ElementTransformer();
                transformer.getComponents().addAll(newElement.getTransformFunctions());
                updateTransformerMap(migration.getElementType(), migration.getOldGroup(), transformer, viewWithTransformationsAndFilters);
            }
            if (CollectionUtils.isNotEmpty(newElement.getPostTransformFilterFunctions())) {
                ElementFilter filter = new ElementFilter.Builder()
                        .select(ElementTuple.ELEMENT)
                        .execute(new TransformAndFilter(migration.getToNewTransform(), newElement.getPostTransformFilter()))
                        .build();
                updatePostTransformFilters(migration.getElementType(), migration.getOldGroup(), filter, viewWithTransformationsAndFilters);
            }
        } else {
            viewBuilder.addTransformFunctions(migration.getToNew());

            if (CollectionUtils.isNotEmpty(newElement.getTransformFunctions())) {
                ElementTransformer transformer = new ElementTransformer();
                transformer.getComponents().addAll(newElement.getTransformFunctions());
                updateTransformerMap(migration.getElementType(), migration.getNewGroup(), transformer, viewWithTransformationsAndFilters);
            }
            if (CollectionUtils.isNotEmpty(newElement.getPostTransformFilterFunctions())) {
                updatePostTransformFilters(migration.getElementType(), migration.getNewGroup(), newElement.getPostTransformFilter(), viewWithTransformationsAndFilters);
            }
        }

        updateElementDefinition(viewBuilder, newElement);
        viewWithTransformationsAndFilters.setViewElementDefinition(viewBuilder.build());

        return viewWithTransformationsAndFilters;
    }

    private void updateElementDefinition(ViewElementDefinition.Builder viewBuilder, ViewElementDefinition elementDefinition) {
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

    private void updateAllFiltersAndTransforms(final MigrateElement.ElementType migrationElementType, final String migrationGroup, final ViewElementDefinition elementDefinition, final ViewWithTransformationsAndFilters viewWithTransformationsAndFilters) {
        if (CollectionUtils.isNotEmpty(elementDefinition.getPostAggregationFilterFunctions())) {
            updatePostAggregationFilters(migrationElementType, migrationGroup, elementDefinition.getPostAggregationFilter(), viewWithTransformationsAndFilters);
            viewWithTransformationsAndFilters.getViewElementDefinition().setPostAggregationFilter(null);
        }
        if (CollectionUtils.isNotEmpty(elementDefinition.getPostTransformFilterFunctions())) {
            updatePostTransformFilters(migrationElementType, migrationGroup, elementDefinition.getPostTransformFilter(), viewWithTransformationsAndFilters);
            viewWithTransformationsAndFilters.getViewElementDefinition().setPostTransformFilter(null);
        }
        if (CollectionUtils.isNotEmpty(elementDefinition.getTransformFunctions())) {
            updateTransformerMap(migrationElementType, migrationGroup, elementDefinition.getTransformer(), viewWithTransformationsAndFilters);
            viewWithTransformationsAndFilters.getViewElementDefinition().setTransformer(null);
        }
    }

    private void updatePostTransformFilters(final MigrateElement.ElementType migrationElementType, final String migrationGroup, final ElementFilter filter, final ViewWithTransformationsAndFilters viewWithTransformationsAndFilters) {
        if (CollectionUtils.isNotEmpty(filter.getComponents())) {
            if (migrationElementType.equals(MigrateElement.ElementType.ENTITY)) {
                viewWithTransformationsAndFilters.getEntitiesPostTransformFilterMap().put(migrationGroup, filter);
            } else if (migrationElementType.equals(MigrateElement.ElementType.EDGE)) {
                viewWithTransformationsAndFilters.getEdgesPostTransformFilterMap().put(migrationGroup, filter);
            }
        }
    }

    private void updatePostAggregationFilters(final MigrateElement.ElementType migrationElementType, final String migrationGroup, final ElementFilter filter, final ViewWithTransformationsAndFilters viewWithTransformationsAndFilters) {
        if (CollectionUtils.isNotEmpty(filter.getComponents())) {
            if (migrationElementType.equals(MigrateElement.ElementType.ENTITY)) {
                viewWithTransformationsAndFilters.getEntitiesPostAggregationFilterMap().put(migrationGroup, filter);
            } else if (migrationElementType.equals(MigrateElement.ElementType.EDGE)) {
                viewWithTransformationsAndFilters.getEdgesPostAggregationFilterMap().put(migrationGroup, filter);
            }
        }
    }

    private void updateTransformerMap(final MigrateElement.ElementType migrationElementType, final String migrationGroup, final ElementTransformer transformer, final ViewWithTransformationsAndFilters viewWithTransformationsAndFilters) {
        if (CollectionUtils.isNotEmpty(transformer.getComponents())) {
            if (migrationElementType.equals(MigrateElement.ElementType.ENTITY)) {
                viewWithTransformationsAndFilters.getEntitiesTransformerMap().put(migrationGroup, transformer);
            } else if (migrationElementType.equals(MigrateElement.ElementType.EDGE)) {
                viewWithTransformationsAndFilters.getEdgesTransformerMap().put(migrationGroup, transformer);
            }
        }
    }

    private ViewWithTransformationsAndFilters getTransformationsAndFilters(final Map<String, ViewWithTransformationsAndFilters> migratedEdges, final Map<String, ViewWithTransformationsAndFilters> migratedEntities) {
        final ViewWithTransformationsAndFilters transformationsAndFilters = new ViewWithTransformationsAndFilters();
        for (Map.Entry<String, ViewWithTransformationsAndFilters> entry : migratedEntities.entrySet()) {
            transformationsAndFilters.setEdgesPostAggregationFilterMap(entry.getValue().getEdgesPostAggregationFilterMap());
            transformationsAndFilters.setEntitiesPostAggregationFilterMap(entry.getValue().getEntitiesPostAggregationFilterMap());
            transformationsAndFilters.setEdgesPostTransformFilterMap(entry.getValue().getEdgesPostTransformFilterMap());
            transformationsAndFilters.setEntitiesPostTransformFilterMap(entry.getValue().getEntitiesPostTransformFilterMap());
            transformationsAndFilters.setEdgesTransformerMap(entry.getValue().getEdgesTransformerMap());
            transformationsAndFilters.setEntitiesTransformerMap(entry.getValue().getEntitiesTransformerMap());
        }
        for (Map.Entry<String, ViewWithTransformationsAndFilters> entry : migratedEdges.entrySet()) {
            transformationsAndFilters.setEdgesPostAggregationFilterMap(entry.getValue().getEdgesPostAggregationFilterMap());
            transformationsAndFilters.setEntitiesPostAggregationFilterMap(entry.getValue().getEntitiesPostAggregationFilterMap());
            transformationsAndFilters.setEdgesPostTransformFilterMap(entry.getValue().getEdgesPostTransformFilterMap());
            transformationsAndFilters.setEntitiesPostTransformFilterMap(entry.getValue().getEntitiesPostTransformFilterMap());
            transformationsAndFilters.setEdgesTransformerMap(entry.getValue().getEdgesTransformerMap());
            transformationsAndFilters.setEntitiesTransformerMap(entry.getValue().getEntitiesTransformerMap());
        }
        return transformationsAndFilters;
    }

    /**
     * POJO to hold all relevant ElementFilters and ElementTransformers with the related ViewElementDefinition.
     */
    private class ViewWithTransformationsAndFilters {
        private ViewElementDefinition viewElementDefinition;
        private Map<String, ElementFilter> entitiesPostAggregationFilterMap;
        private Map<String, ElementFilter> edgesPostAggregationFilterMap;
        private Map<String, ElementTransformer> entitiesTransformerMap;
        private Map<String, ElementTransformer> edgesTransformerMap;
        private Map<String, ElementFilter> entitiesPostTransformFilterMap;
        private Map<String, ElementFilter> edgesPostTransformFilterMap;

        public ViewWithTransformationsAndFilters() {
            viewElementDefinition = new ViewElementDefinition();
            entitiesPostAggregationFilterMap = new HashMap<>();
            edgesPostAggregationFilterMap = new HashMap<>();
            entitiesTransformerMap = new HashMap<>();
            edgesTransformerMap = new HashMap<>();
            entitiesPostTransformFilterMap = new HashMap<>();
            edgesPostTransformFilterMap = new HashMap<>();
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

        public void setEntitiesPostAggregationFilterMap(final Map<String, ElementFilter> entitiesPostAggregationFilterMap) {
            if (this.entitiesPostAggregationFilterMap.isEmpty()) {
                this.entitiesPostAggregationFilterMap = entitiesPostAggregationFilterMap;
            } else {
                this.entitiesPostAggregationFilterMap.putAll(entitiesPostAggregationFilterMap);
            }
        }

        public Map<String, ElementFilter> getEdgesPostAggregationFilterMap() {
            return edgesPostAggregationFilterMap;
        }

        public void setEdgesPostAggregationFilterMap(final Map<String, ElementFilter> edgesPostAggregationFilterMap) {
            if (edgesPostAggregationFilterMap.isEmpty()) {
                this.edgesPostAggregationFilterMap = edgesPostAggregationFilterMap;
            } else {
                this.edgesPostAggregationFilterMap.putAll(edgesPostAggregationFilterMap);
            }
        }

        public Map<String, ElementTransformer> getEntitiesTransformerMap() {
            return entitiesTransformerMap;
        }

        public void setEntitiesTransformerMap(final Map<String, ElementTransformer> entitiesTransformerMap) {
            if (this.entitiesTransformerMap.isEmpty()) {
                this.entitiesTransformerMap = entitiesTransformerMap;
            } else {
                this.entitiesTransformerMap.putAll(entitiesTransformerMap);
            }
        }

        public Map<String, ElementTransformer> getEdgesTransformerMap() {
            return edgesTransformerMap;
        }

        public void setEdgesTransformerMap(final Map<String, ElementTransformer> edgesTransformerMap) {
            if (this.edgesTransformerMap.isEmpty()) {
                this.edgesTransformerMap = edgesTransformerMap;
            } else {
                this.edgesTransformerMap.putAll(edgesTransformerMap);
            }
        }

        public Map<String, ElementFilter> getEntitiesPostTransformFilterMap() {
            return entitiesPostTransformFilterMap;
        }

        public void setEntitiesPostTransformFilterMap(final Map<String, ElementFilter> entitiesPostTransformFilterMap) {
            if (this.entitiesPostTransformFilterMap.isEmpty()) {
                this.entitiesPostTransformFilterMap = entitiesPostTransformFilterMap;
            } else {
                this.entitiesPostTransformFilterMap.putAll(entitiesPostTransformFilterMap);
            }
        }

        public Map<String, ElementFilter> getEdgesPostTransformFilterMap() {
            return edgesPostTransformFilterMap;
        }

        public void setEdgesPostTransformFilterMap(final Map<String, ElementFilter> edgesPostTransformFilterMap) {
            if (this.edgesPostTransformFilterMap.isEmpty()) {
                this.edgesPostTransformFilterMap = edgesPostTransformFilterMap;
            } else {
                this.edgesPostTransformFilterMap.putAll(edgesPostTransformFilterMap);
            }
        }
    }
}
