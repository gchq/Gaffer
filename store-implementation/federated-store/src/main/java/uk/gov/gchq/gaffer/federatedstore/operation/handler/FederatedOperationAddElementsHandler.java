/*
 * Copyright 2017 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.operation.handler;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

import java.util.Collection;
import java.util.Set;

import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.KEY_SKIP_FAILED_FEDERATED_STORE_EXECUTE;

/**
 * A handler for AddElements operation for the FederatedStore.
 * <p>
 * Only attempts to add elements to a graph if it has knowledge of the elements
 * edge/entity group name.
 * Otherwise will throw exception if all the following is true.
 * <ul>
 * <li> If no sub-graphs have knowledge of the elements edge/entity group name.
 * <li> isSkipInvalidElements flag is false.
 * </ul>
 *
 * @see OperationHandler
 * @see FederatedStore
 * @see AddElements
 */
public class FederatedOperationAddElementsHandler implements OperationHandler<AddElements> {
    public Object doOperation(final AddElements addElements, final Context context, final Store store) throws OperationException {
        final Set<String> allGroups = store.getSchema().getGroups();
        final Collection<Graph> graphs = ((FederatedStore) store).getGraphs(context.getUser(), addElements.getOption(KEY_OPERATION_OPTIONS_GRAPH_IDS));

        for (final Graph graph : graphs) {
            final Set<String> graphGroups = graph.getSchema().getGroups();
            final Iterable<? extends Element> retain = Iterables.filter(
                    addElements.getInput(),
                    forUnknownGroupSkipForGraphButThrowForWholeStore(
                            graphGroups,
                            allGroups,
                            addElements.isSkipInvalidElements()
                    )
            );
            try {
                final AddElements addElementsClone = addElements.shallowClone();
                addElementsClone.setInput(retain);
                graph.execute(addElementsClone, context.getUser());
            } catch (final Exception e) {
                if (!Boolean.valueOf(addElements.getOption(KEY_SKIP_FAILED_FEDERATED_STORE_EXECUTE))) {
                    throw new OperationException("Graph failed to execute operation. Graph: " + graph.getGraphId() + " Operation: " + addElements.getClass().getSimpleName(), e);
                }
            }
        }
        return null;
    }

    private Predicate<Element> forUnknownGroupSkipForGraphButThrowForWholeStore(final Set<String> graphGroups, final Set<String> allGroups, final boolean skipInvalidElements) {
        return element -> {
            String elementGroup = null != element ? element.getGroup() : null;
            boolean graphContainsGroup = graphGroups.contains(elementGroup);
            if (!graphContainsGroup
                    && !allGroups.contains(elementGroup)
                    && !skipInvalidElements) {
                throw new IllegalArgumentException("Element has an unknown group: " + element);
            }
            return graphContainsGroup;
        };
    }
}
