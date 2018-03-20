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

package uk.gov.gchq.gaffer.graph.hook;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.collections.CollectionUtils;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.ElementTuple;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.function.migration.MigrateElement;
import uk.gov.gchq.gaffer.operation.graph.OperationView;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.koryphe.predicate.KoryphePredicate;

import java.util.ArrayList;
import java.util.List;

@JsonPropertyOrder(value = {"migrations", "transformToNew"}, alphabetic = true)
public class SchemaMigration implements GraphHook {
    private List<MigrateElement> entities = new ArrayList<>();
    private List<MigrateElement> edges = new ArrayList<>();

    private boolean transformToNew = true;

    @Override
    public void preExecute(final OperationChain<?> opChain, final Context context) {
        if (!edges.isEmpty() || !entities.isEmpty()) {
            for (final Operation op : opChain.flatten()) {
                if (op instanceof OperationView) {
                    updateView((OperationView) op);
                }
            }
        }
    }

    @Override
    public <T> T postExecute(final T result, final OperationChain<?> opChain, final Context context) {
        return result;
    }

    @Override
    public <T> T onFailure(final T result, final OperationChain<?> opChain, final Context context, final Exception e) {
        return result;
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

    public boolean isTransformToNew() {
        return transformToNew;
    }

    public void setTransformToNew(final boolean transformToNew) {
        this.transformToNew = transformToNew;
    }

    public static class TransformAndFilter extends KoryphePredicate<Element> {
        private ElementTransformer transformer;
        private ElementFilter filter;

        public TransformAndFilter() {
        }

        public TransformAndFilter(final ElementFilter filter) {
            this.filter = filter;
        }

        public TransformAndFilter(final ElementTransformer transformer, final ElementFilter filter) {
            this.transformer = transformer;
            this.filter = filter;
        }

        @Override
        public boolean test(final Element element) {
            return null == filter || filter.test(null != transformer ? transformer.apply(element) : element);
        }

        public ElementTransformer getTransformer() {
            return transformer;
        }

        public void setTransformer(final ElementTransformer transformer) {
            this.transformer = transformer;
        }

        public ElementFilter getFilter() {
            return filter;
        }

        public void setFilter(final ElementFilter filter) {
            this.filter = filter;
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("transformer", transformer)
                    .append("filter", filter)
                    .toString();
        }
    }

    private void updateView(final OperationView op) {
        final View currentView = op.getView();
        final View.Builder newViewBuilder = new View.Builder().merge(currentView);
        for (final MigrateElement migration : edges) {
            ViewElementDefinition oldElement = currentView.getEdge(migration.getOldGroup());
            if (null != oldElement) {
                ViewElementDefinition newElement = currentView.getEdge(migration.getNewGroup());
                if (null == newElement || newElement.isEmpty()) {
                    if (migration.getToNew().isEmpty()) {
                        newElement = oldElement;
                    } else {
                        newElement = createNewElementDef(migration, oldElement);
                    }
                    newViewBuilder.edge(migration.getNewGroup(), newElement);
                }
                if (transformToNew && !migration.getToNew().isEmpty()) {
                    newViewBuilder.edge(migration.getOldGroup(), createOldElementDef(migration, oldElement));
                }
            }
        }

        for (final MigrateElement migration : entities) {
            final ViewElementDefinition oldElement = currentView.getEntity(migration.getOldGroup());
            if (null != oldElement) {
                ViewElementDefinition newElement = currentView.getEntity(migration.getNewGroup());
                if (null == newElement || newElement.isEmpty()) {
                    if (migration.getToNew().isEmpty()) {
                        newElement = oldElement;
                    } else {
                        newElement = createNewElementDef(migration, oldElement);
                    }
                    newViewBuilder.entity(migration.getNewGroup(), newElement);
                }
                if (transformToNew && !migration.getToNew().isEmpty()) {
                    newViewBuilder.entity(migration.getOldGroup(), createOldElementDef(migration, oldElement));
                }
            }
        }

        op.setView(newViewBuilder.build());
    }

    private ViewElementDefinition createOldElementDef(final MigrateElement migration, final ViewElementDefinition oldElement) {
        ViewElementDefinition.Builder oldElementBuilder = new ViewElementDefinition.Builder()
                .merge(oldElement)
                .transformFunctions(migration.getToNew());
        if (CollectionUtils.isNotEmpty(oldElement.getPostTransformFilterFunctions())) {
            oldElementBuilder = oldElementBuilder.postTransformFilter(new ElementFilter.Builder()
                    .select(ElementTuple.ELEMENT)
                    .execute(new TransformAndFilter(migration.getToOldTransform(), oldElement.getPostTransformFilter()))
                    .build());
        }
        return oldElementBuilder.build();
    }

    private ViewElementDefinition createNewElementDef(final MigrateElement migration, final ViewElementDefinition oldElement) {
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

        if (transformToNew) {
            if (CollectionUtils.isNotEmpty(oldElement.getTransformFunctions())) {
                viewBuilder = viewBuilder.transformFunctions(migration.getToOld())
                        .transformFunctions(oldElement.getTransformFunctions())
                        .transformFunctions(migration.getToNew());
            }

            if (CollectionUtils.isNotEmpty(oldElement.getPostTransformFilterFunctions())) {
                viewBuilder = viewBuilder.postTransformFilter(new ElementFilter.Builder()
                        .select(ElementTuple.ELEMENT)
                        .execute(new TransformAndFilter(migration.getToOldTransform(), oldElement.getPostTransformFilter()))
                        .build());
            }
        } else {
            viewBuilder = viewBuilder.transformFunctions(migration.getToOld());

            if (CollectionUtils.isNotEmpty(oldElement.getPostTransformFilterFunctions())) {
                viewBuilder = viewBuilder.postTransformFilter(oldElement.getPostTransformFilter());
            }
        }

        //TODO apply all of the other ViewElementDefinition fields

        return viewBuilder.build();
    }
}
