/*
 * Copyright 2017-2021 Crown Copyright
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
package uk.gov.gchq.gaffer.federatedstore.util;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.Operations;
import uk.gov.gchq.gaffer.operation.graph.OperationView;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.operation.GetTraits;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.impl.binaryoperator.IterableConcat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static java.util.Objects.nonNull;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.KEY_SKIP_FAILED_FEDERATED_STORE_EXECUTE;

public final class FederatedStoreUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(FederatedStoreUtil.class);
    private static final String SCHEMA_DEL_REGEX = Pattern.quote(",");
    public static final Collection<String> STRINGS_TO_REMOVE = Collections.unmodifiableCollection(Arrays.asList("", null));

    private FederatedStoreUtil() {
    }

    public static String createOperationErrorMsg(final Operation operation, final String graphId, final Exception e) {
        final String additionalInfo = String.format("Set the skip and continue flag: %s for operation: %s",
                KEY_SKIP_FAILED_FEDERATED_STORE_EXECUTE,
                operation.getClass().getSimpleName());

        return String.format("Failed to execute %s on graph %s.%n %s.%n Error: %s",
                operation.getClass().getSimpleName(), graphId, additionalInfo, e.getMessage());
    }

    public static List<String> getCleanStrings(final String value) {
        final List<String> values;
        if (value != null) {
            values = Lists.newArrayList(StringUtils.stripAll(value.split(SCHEMA_DEL_REGEX)));
            values.removeAll(STRINGS_TO_REMOVE);
        } else {
            values = null;
        }
        return values;
    }

    /**
     * <p>
     * Within FederatedStore an {@link Operation} is executed against a
     * collection of many graphs.
     * </p>
     * <p>
     * Problem: When an Operation contains View information about an Element
     * which is not known by the Graph; It will fail validation when executed.
     * </p>
     * <p>
     * Solution: For each operation, remove all elements from the View that is
     * unknown to the graph. This method will also update AddElements operations
     * to allow elements to be added to various federated graphs with different
     * schemas at the same time without causing validation errors.
     * </p>
     *
     * @param operation current operation
     * @param graph     current graph
     * @param <OP>      Operation type
     * @return cloned operation with modified View for the given graph.
     */
    public static <OP extends Operation> OP updateOperationForGraph(final OP operation, final Graph graph) {
        OP resultOp = operation;
        if (operation instanceof Operations) {
            resultOp = (OP) operation.shallowClone();
            final Operations<Operation> operations = (Operations) resultOp;
            final List<Operation> resultOperations = new ArrayList<>();
            for (final Operation nestedOp : operations.getOperations()) {
                final Operation updatedNestedOp = updateOperationForGraph(nestedOp, graph);
                if (null == updatedNestedOp) {
                    resultOp = null;
                    break;
                }
                resultOperations.add(updatedNestedOp);
            }
            operations.updateOperations(resultOperations);
        } else if (operation instanceof OperationView) {
            final View view = ((OperationView) operation).getView();
            if (null != view && view.hasGroups()) {
                final View validView = createValidView(view, graph.getSchema());
                if (view != validView) {
                    // If the view is not the same instance as the original view
                    // then clone the operation and add the new view.
                    resultOp = (OP) operation.shallowClone();
                    if (validView.hasGroups()) {
                        ((OperationView) resultOp).setView(validView);
                    } else if (!graph.hasTrait(StoreTrait.DYNAMIC_SCHEMA)) {
                        // The view has no groups so the operation would return
                        // nothing, so we shouldn't execute the operation.
                        resultOp = null;
                    }
                }
            }
        } else if (operation instanceof AddElements) {
            final AddElements addElements = ((AddElements) operation);
            if (null == addElements.getInput()) {
                if (!addElements.isValidate() || !addElements.isSkipInvalidElements()) {
                    LOGGER.debug("Invalid elements will be skipped when added to {}", graph.getGraphId());
                    resultOp = (OP) addElements.shallowClone();
                    ((AddElements) resultOp).setValidate(true);
                    ((AddElements) resultOp).setSkipInvalidElements(true);
                }
            } else {
                resultOp = (OP) addElements.shallowClone();
                final Set<String> graphGroups = graph.getSchema().getGroups();
                final Iterable<? extends Element> filteredInput = Iterables.filter(
                        addElements.getInput(),
                        element -> graphGroups.contains(null != element ? element.getGroup() : null)
                );
                ((AddElements) resultOp).setInput(filteredInput);
            }
        }

        return resultOp;
    }

    private static View createValidView(final View view, final Schema delegateGraphSchema) {
        View newView = view;
        if (view.hasGroups()) {
            final Set<String> validEntities = new HashSet<>(view.getEntityGroups());
            final Set<String> validEdges = new HashSet<>(view.getEdgeGroups());
            validEntities.retainAll(delegateGraphSchema.getEntityGroups());
            validEdges.retainAll(delegateGraphSchema.getEdgeGroups());

            if (!validEntities.equals(view.getEntityGroups()) || !validEdges.equals(view.getEdgeGroups())) {
                // Need to make changes to the view so start by cloning the view
                // and clearing all the edges and entities
                final View.Builder viewBuilder = new View.Builder()
                        .merge(view)
                        .entities(Collections.emptyMap())
                        .edges(Collections.emptyMap());
                validEntities.forEach(e -> viewBuilder.entity(e, view.getEntity(e)));
                validEdges.forEach(e -> viewBuilder.edge(e, view.getEdge(e)));
                newView = viewBuilder.build();
            }
        }
        return newView;
    }

    //TODO FS Examine
    public static boolean isUserRequestingAdminUsage(final Operation operation) {
        return Boolean.parseBoolean(operation.getOption(FederatedStoreConstants.KEY_FEDERATION_ADMIN, "false"));
    }

    /**
     * Defaulted with a iterableConcat
     *
     * @param operation operation to be wrapped in FederatedOperation
     * @param <PAYLOAD> The operation type
     * @return the wrapped operation
     */
    public static <PAYLOAD extends Operation> FederatedOperation<PAYLOAD> getFederatedOperation(final PAYLOAD operation) {

        FederatedOperation.Builder<PAYLOAD> builder = new FederatedOperation.Builder<PAYLOAD>()
                .op(operation)
                .mergeFunction(new IterableConcat())
                //TODO FS Examine
//                .graphIds(default)
//                .graphIds(operation.getOption(FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS))
                //TODO FS Examine
                .options(operation.getOptions());

        String graphIdOption = operation.getOption("gaffer.federatedstore.operation.graphIds");
        if (nonNull(graphIdOption)) {
            //TODO FS Examine, ignore or copy or Error
            LOGGER.info("Operation:{} has old deprecated style of graphId selection. Ignoring:{}", operation.getClass().getSimpleName(), graphIdOption);
            // LOGGER.info("Operation:{} has old deprecated style of graphId selection.", operation.getClass().getSimpleName());
            builder.graphIds(graphIdOption);
        }

        return builder.build();

    }

    public static FederatedOperation<GetSchema> getFederatedWrappedSchema() {
        return getFederatedOperation(new GetSchema());
    }

    public static FederatedOperation<GetTraits> getFederatedWrappedTraits() {
        return getFederatedOperation(new GetTraits());
    }
}
