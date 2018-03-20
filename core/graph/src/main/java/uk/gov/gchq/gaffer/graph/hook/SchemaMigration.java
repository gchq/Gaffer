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

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.ElementTuple;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.function.migration.MigrateElement;
import uk.gov.gchq.gaffer.operation.function.migration.MigrateElements;
import uk.gov.gchq.gaffer.operation.graph.OperationView;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.koryphe.predicate.KoryphePredicate;

@JsonPropertyOrder(alphabetic = true)
public class SchemaMigration implements GraphHook {
    private MigrateElements migrateElements;

    public MigrateElements getMigrateElements() {
        return migrateElements;
    }

    public void setMigrateElements(final MigrateElements migrateElements) {
        this.migrateElements = migrateElements;
    }

    @Override
    public void preExecute(final OperationChain<?> opChain, final Context context) {
        if (!migrateElements.getEdges().isEmpty() || !migrateElements.getEdges().isEmpty()) {
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
    }

    private View updateView(final OperationView op) {
        final View currentView = op.getView();
        final View.Builder newViewBuilder = new View.Builder().merge(currentView);
        for (final MigrateElement migration : migrateElements.getEdges()) {
            final ViewElementDefinition oldElement = currentView.getEdge(migration.getOldGroup());
            ViewElementDefinition newElement = currentView.getEdge(migration.getNewGroup());
            if (null == newElement || newElement.isEmpty()) {
                if (migration.getToNew().isEmpty()) {
                    newElement = oldElement;
                } else {
                    newElement = createNewElementDef(migration, oldElement);
                }
                newViewBuilder.edge(migration.getNewGroup(), newElement);
            }
        }

        for (final MigrateElement migration : migrateElements.getEntities()) {
            final ViewElementDefinition oldElement = currentView.getEntity(migration.getOldGroup());
            ViewElementDefinition newElement = currentView.getEntity(migration.getNewGroup());
            if (null == newElement || newElement.isEmpty()) {
                if (migration.getToNew().isEmpty()) {
                    newElement = oldElement;
                } else {
                    newElement = createNewElementDef(migration, oldElement);
                }
                newViewBuilder.entity(migration.getNewGroup(), newElement);
            }
        }

        return newViewBuilder.build();
    }

    private ViewElementDefinition createNewElementDef(final MigrateElement migration, final ViewElementDefinition oldElement) {
        return new ViewElementDefinition.Builder()
                .preAggregationFilter(new ElementFilter.Builder()
                        .select(ElementTuple.ELEMENT)
                        .execute(new TransformAndFilter(migration.getToOldTransform(), oldElement.getPreAggregationFilter()))
                        .build())
                .postAggregationFilter(new ElementFilter.Builder()
                        .select(ElementTuple.ELEMENT)
                        .execute(new TransformAndFilter(migration.getToOldTransform(), oldElement.getPostAggregationFilter()))
                        .build())
                .transformFunctions(migration.getToOld())
                .transformFunctions(oldElement.getTransformFunctions())
                .transformFunctions(migration.getToNew())
                .postTransformFilter(new ElementFilter.Builder()
                        .select(ElementTuple.ELEMENT)
                        .execute(new TransformAndFilter(migration.getToOldTransform(), oldElement.getPostAggregationFilter()))
                        .build())
                .build();
    }
}
