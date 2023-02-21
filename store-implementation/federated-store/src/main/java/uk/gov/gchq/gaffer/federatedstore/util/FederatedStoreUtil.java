/*
 * Copyright 2017-2023 Crown Copyright
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

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.Iterables;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.core.exception.GafferCheckedException;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.Operations;
import uk.gov.gchq.gaffer.operation.graph.OperationView;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.io.Input;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static uk.gov.gchq.gaffer.federatedstore.util.ApplyViewToElementsFunction.SCHEMA;
import static uk.gov.gchq.gaffer.federatedstore.util.ApplyViewToElementsFunction.USER;
import static uk.gov.gchq.gaffer.federatedstore.util.ApplyViewToElementsFunction.VIEW;

public final class FederatedStoreUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(FederatedStoreUtil.class);
    private static final String SCHEMA_DEL_REGEX = Pattern.quote(",");

    @Deprecated
    public static final String DEPRECATED_GRAPHIDS_OPTION = "gaffer.federatedstore.operation.graphIds";

    private FederatedStoreUtil() {
    }

    public static String createOperationErrorMsg(final Operation operation, final String graphId, final Exception e) {
        final String additionalInfo = String.format("Set the skip and continue option: skipFailedFederatedExecution for operation: %s",
                operation.getClass().getSimpleName());

        return String.format("Failed to execute %s on graph %s.%n %s.%n Error: %s",
                operation.getClass().getSimpleName(), graphId, additionalInfo, e.getMessage());
    }

    public static List<String> getCleanStrings(final String value) {
        final List<String> values;
        if (value != null) {
            values = Arrays.stream(StringUtils.stripAll(value.split(SCHEMA_DEL_REGEX)))
                    .filter(StringUtils::isNotBlank)
                    .collect(Collectors.toList());
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
     * @param <OP>      Operation type
     * @param operation current operation
     * @param graph     current graph
     * @param context   current context, used for getSchema operation
     * @return cloned operation with modified View for the given graph.
     */
    public static <OP extends Operation> OP updateOperationForGraph(final OP operation, final Graph graph, final Context context) {
        OP resultOp = (OP) operation.shallowClone();
        if (nonNull(resultOp.getOptions())) {
            resultOp.setOptions(new HashMap<>(resultOp.getOptions()));
        }
        if (resultOp instanceof Operations) {
            final Operations<Operation> operations = (Operations) resultOp;
            final List<Operation> resultOperations = new ArrayList<>();
            for (final Operation nestedOp : operations.getOperations()) {
                final Operation updatedNestedOp = updateOperationForGraph(nestedOp, graph, context);
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
                try {
                    final Set<String> graphGroups = graph.execute(new GetSchema(), context).getGroups();
                    final Iterable<? extends Element> filteredInput = Iterables.filter(
                            addElements.getInput(),
                            element -> graphGroups.contains(null != element ? element.getGroup() : null)
                    );
                    ((AddElements) resultOp).setInput(filteredInput);
                } catch (final Exception e) {
                    LOGGER.error("Error getting schema to filter Input based on legal groups for the graphId={}. Will attempt with No input filtering. Error was due to: {}", graph.getGraphId(), e.getMessage());
                    ((AddElements) resultOp).setInput(addElements.getInput());
                }
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
                .op(operation);

        addDeprecatedGraphIds(operation, builder);


        return builder.build();
    }

    public static BiFunction getDefaultMergeFunction() {
        return new ConcatenateMergeFunction();
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
            builder.graphIdsCSV(graphIdOption);
        }
        return builder;
    }

    @Deprecated
    public static String getDeprecatedGraphIds(final Operation operation) throws GafferRuntimeException {
        String deprecatedGraphIds = operation.getOption(DEPRECATED_GRAPHIDS_OPTION);
        if (nonNull(deprecatedGraphIds)) {
            String simpleName = operation.getClass().getSimpleName();
            LOGGER.warn("Operation:{} has old Deprecated style of graphId selection.", simpleName);
        }
        return deprecatedGraphIds;
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

    public static BiFunction processIfFunctionIsContextSpecific(final BiFunction mergeFunction, final Operation payload, final Context operationContext, final List<String> graphIds, final FederatedStore federatedStore) {
        final BiFunction rtn;
        if (!(mergeFunction instanceof ContextSpecificMergeFunction)) {
            rtn = mergeFunction;
        } else {
            try {
                final ContextSpecificMergeFunction specificMergeFunction = (ContextSpecificMergeFunction) mergeFunction;
                HashMap<String, Object> functionContext = new HashMap<>();

                functionContext = processSchemaForSpecificMergeFunction(specificMergeFunction, functionContext, payload, graphIds, operationContext, federatedStore);
                functionContext = processViewForSpecificMergeFunction(specificMergeFunction, functionContext, payload);
                functionContext = processUserForSpecificMergeFunction(specificMergeFunction, functionContext, operationContext.getUser());

                //This line creates a ContextSpecificMergeFunction based on the given context.
                rtn = specificMergeFunction.createFunctionWithContext(functionContext);
            } catch (final Exception e) {
                throw new GafferRuntimeException("Error getting ContextSpecificMergeFunction, due to:" + e.getMessage(), e);
            }
        }
        return rtn;
    }

    private static HashMap<String, Object> processViewForSpecificMergeFunction(final ContextSpecificMergeFunction specificMergeFunction, final HashMap<String, Object> functionContext, final Operation payload) throws GafferCheckedException {
        if (specificMergeFunction.isRequired(VIEW)) {
            try {
                functionContext.put(VIEW, ((OperationView) payload).getView());
            } catch (final ClassCastException e) {
                throw new GafferCheckedException("Merge function requires a view for payload operation, " +
                        "but it is not an instance of OperationView, likely the wrong merge function has been asked. payload:" + payload.getClass(), e);
            }
        }
        return functionContext;
    }

    private static HashMap<String, Object> processSchemaForSpecificMergeFunction(final ContextSpecificMergeFunction specificMergeFunction, final HashMap<String, Object> functionContext, final Operation payload, final List<String> graphIds, final Context operationContext, final FederatedStore federatedStore) throws GafferCheckedException {
        if (specificMergeFunction.isRequired(SCHEMA)) {
            if (payload instanceof GetSchema) {
                throw new UnsupportedOperationException(String.format("Infinite Loop Error: %s Operation can not be configured " +
                        "with a merge that internally performs the same operation, check the Admin configuredDefaultMergeFunctions. Configured MergeFunction:%s", GetSchema.class.getSimpleName(), specificMergeFunction.getClass()));
            }

            try {
                final Schema schema = (Schema) federatedStore.execute(new FederatedOperation.Builder()
                        .op(new GetSchema())
                        .graphIds(graphIds)
                        .build(), operationContext);

                functionContext.put(SCHEMA, schema);
            } catch (final OperationException e) {
                throw new GafferCheckedException(String.format("Error getting Schema for SpecificMergeFunction, due to:%s", e.getMessage()), e);
            }
        }
        return functionContext;
    }

    private static HashMap<String, Object> processUserForSpecificMergeFunction(final ContextSpecificMergeFunction specificMergeFunction, final HashMap<String, Object> functionContext, final User user) throws GafferCheckedException {
        if (specificMergeFunction.isRequired(USER)) {
            functionContext.put(USER, user);
        }
        return functionContext;
    }

    public static BiFunction getStoreConfiguredMergeFunction(final Operation payload, final Context context, final List graphIds, final FederatedStore store) throws GafferCheckedException {
        try {
            BiFunction mergeFunction = isNull(store.getStoreConfiguredMergeFunctions()) || isNull(payload)
                    ? getDefaultMergeFunction()
                    : store.getStoreConfiguredMergeFunctions().getOrDefault(payload.getClass().getCanonicalName(), getDefaultMergeFunction());
            return processIfFunctionIsContextSpecific(mergeFunction, payload, context, graphIds, store);
        } catch (final Exception e) {
            throw new GafferRuntimeException("Error getting default merge function, due to:" + e.getMessage(), e);
        }
    }

    public static List<String> loadStoreConfiguredGraphIdsListFrom(final String pathStr) throws IOException {
        if (isNull(pathStr)) {
            return null;
        }
        final Path path = Paths.get(pathStr);
        byte[] json;
        if (path.toFile().exists()) {
            json = Files.readAllBytes(path);
        } else {
            json = IOUtils.toByteArray(StreamUtil.openStream(FederatedStoreUtil.class, pathStr));
        }
        return JSONSerialiser.deserialise(json, List.class);
    }

    public static Map<String, BiFunction> loadStoreConfiguredMergeFunctionMapFrom(final String pathStr) throws IOException {
        if (isNull(pathStr)) {
            return Collections.emptyMap();
        }
        final Path path = Paths.get(pathStr);
        byte[] json;
        if (path.toFile().exists()) {
            json = Files.readAllBytes(path);
        } else {
            json = IOUtils.toByteArray(StreamUtil.openStream(FederatedStoreUtil.class, pathStr));
        }
        return JSONSerialiser.deserialise(json, SerialisableConfiguredMergeFunctionsMap.class).getMap();
    }

    public static class SerialisableConfiguredMergeFunctionsMap {
        @JsonProperty("configuredMergeFunctions")
        @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "class")
        HashMap<String, BiFunction> map = new HashMap<>();

        public SerialisableConfiguredMergeFunctionsMap() {
        }

        @JsonGetter("configuredMergeFunctions")
        public HashMap<String, BiFunction> getMap() {
            return map;
        }

        @JsonSetter("configuredMergeFunctions")
        public void setMap(final HashMap<String, BiFunction> map) {
            this.map = map;
        }


    }

}
