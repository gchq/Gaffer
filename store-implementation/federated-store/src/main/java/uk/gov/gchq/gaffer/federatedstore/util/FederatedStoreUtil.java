/*
 * Copyright 2017-2022 Crown Copyright
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

import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.Operations;
import uk.gov.gchq.gaffer.operation.graph.OperationView;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.io.Input;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.regex.Pattern;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public final class FederatedStoreUtil {
    public static final Collection<String> STRINGS_TO_REMOVE = Collections.unmodifiableCollection(Arrays.asList("", null));
    public static final String DEPRECATED_GRAPH_IDS_FLAG = "gaffer.federatedstore.operation.graphIds";
    private static final Logger LOGGER = LoggerFactory.getLogger(FederatedStoreUtil.class);
    private static final String SCHEMA_DEL_REGEX = Pattern.quote(",");

    private FederatedStoreUtil() {
    }

    public static String createOperationErrorMsg(final Operation operation, final String graphId, final Exception e) {
        final String additionalInfo = String.format("Set the skip and continue option: %s for operation: %s",
                "skipFailedFederatedExecution",
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
        OP resultOp = (OP) operation.shallowClone();
        if (nonNull(resultOp.getOptions())) {
            resultOp.setOptions(new HashMap<>(resultOp.getOptions()));
        }
        if (resultOp instanceof Operations) {
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
        } else if (resultOp instanceof OperationView) {
            final View view = ((OperationView) resultOp).getView();
            if (null != view && view.hasGroups()) {
                final View validView = createValidView(view, graph.getSchema());
                if (view != validView) {
                    // If the view is not the same instance as the original view
                    // then clone the operation and add the new view.
                    if (validView.hasGroups()) {
                        ((OperationView) resultOp).setView(validView);
                        // Deprecated function still in use due to Federated GetTraits bug with DYNAMIC_SCHEMA
                    } else if (!graph.getStoreTraits().contains(StoreTrait.DYNAMIC_SCHEMA)) {
                        // The view has no groups so the operation would return
                        // nothing, so we shouldn't execute the operation.
                        resultOp = null;
                    }
                }
            }
        } else if (resultOp instanceof AddElements) {
            final AddElements addElements = ((AddElements) resultOp);
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

    /**
     * Defaulted with a iterableConcat
     *
     * @param operation operation to be wrapped in FederatedOperation
     * @param <INPUT>   payload input type
     * @param <OUTPUT>  merge function output type
     * @return the wrapped operation
     */
    public static <INPUT, OUTPUT extends Iterable<?>> FederatedOperation<INPUT, OUTPUT> getFederatedOperation(final InputOutput<INPUT, OUTPUT> operation) {

        FederatedOperation.BuilderParent<INPUT, OUTPUT> builder = new FederatedOperation.Builder()
                .op(operation);

        addDeprecatedGraphIds(operation, builder);

        return builder.build();
    }


    public static <INPUT> FederatedOperation<INPUT, Void> getFederatedOperation(final Input<INPUT> operation) {
        FederatedOperation.BuilderParent<INPUT, Void> builder = new FederatedOperation.Builder()
                .op(operation);

        addDeprecatedGraphIds(operation, builder);

        return builder.build();
    }

    public static <OUTPUT extends Iterable<?>> FederatedOperation<Void, OUTPUT> getFederatedOperation(final Output<OUTPUT> operation) {

        FederatedOperation.BuilderParent builder = new FederatedOperation.Builder()
                .op(operation)
                .mergeFunction(getHardCodedDefaultMergeFunction());

        addDeprecatedGraphIds(operation, builder);


        return builder.build();
    }

    public static BiFunction getHardCodedDefaultMergeFunction() {
        return new DefaultBestEffortsMergeFunction();
    }

    public static <INPUT> FederatedOperation<INPUT, Void> getFederatedOperation(final Operation operation) {

        FederatedOperation.BuilderParent<INPUT, Void> builder = new FederatedOperation.Builder()
                .op(operation);

        addDeprecatedGraphIds(operation, builder);

        return builder.build();
    }

    @Deprecated
    public static <INPUT, OUTPUT> FederatedOperation.BuilderParent<INPUT, OUTPUT> addDeprecatedGraphIds(final Operation operation, final FederatedOperation.BuilderParent<INPUT, OUTPUT> builder) {
        String graphIdOption = getDeprecatedGraphIds(operation);
        if (nonNull(graphIdOption)) {
            builder.graphIds(graphIdOption);
        }
        return builder;
    }

    @Deprecated
    public static String getDeprecatedGraphIds(final Operation operation) throws GafferRuntimeException {
        String deprecatedGraphIds = operation.getOption(DEPRECATED_GRAPH_IDS_FLAG);
        if (nonNull(deprecatedGraphIds)) {
            String simpleName = operation.getClass().getSimpleName();
            LOGGER.warn("Operation:{} has old Deprecated style of graphId selection.", simpleName);
            //throw new GafferRuntimeException(String.format("Operation:%s has old deprecated style of graphId selection. Use FederatedOperation to perform this selection", simpleName));
        }
        return deprecatedGraphIds;
    }

    @Deprecated
    public static FederatedOperation<Void, Iterable<Schema>> getFederatedWrappedSchema() {
        return new FederatedOperation.Builder().<Void, Iterable<Schema>>op(new GetSchema()).build();
    }

    /**
     * Return a clone of the given operations with a deep clone of options.
     * <p>
     * Because payloadOperation.shallowClone() is used it can't be guaranteed that original options won't be modified.
     * So a deep clone of the options is made for the shallow clone of the operation.
     *
     * @param op the operation to clone
     * @return a clone of the operation with a deep clone of options.
     */
    public static Operation shallowCloneWithDeepOptions(final Operation op) {
        final Operation cloneForValidation = op.shallowClone();
        final Map<String, String> options = op.getOptions();
        final Map<String, String> optionsDeepClone = isNull(options) ? null : new HashMap<>(options);
        cloneForValidation.setOptions(optionsDeepClone);
        return cloneForValidation;
    }
}
