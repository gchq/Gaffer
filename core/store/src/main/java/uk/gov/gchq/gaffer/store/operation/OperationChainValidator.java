/*
 * Copyright 2016-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.graph.GraphFilters;
import uk.gov.gchq.gaffer.operation.impl.compare.ElementComparison;
import uk.gov.gchq.gaffer.operation.io.Input;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import uk.gov.gchq.gaffer.store.schema.ViewValidator;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.Set;

/**
 * Validation class for validating {@link OperationChain}s against {@link ViewValidator}s.
 */
public class OperationChainValidator {
    private static final Logger LOGGER = LoggerFactory.getLogger(OperationChainValidator.class);

    private final ViewValidator viewValidator;

    public OperationChainValidator(final ViewValidator viewValidator) {
        this.viewValidator = viewValidator;
    }

    /**
     * Validate the provided {@link OperationChain} against the {@link ViewValidator}.
     *
     * @param operationChain the operation chain to validate
     * @param user           the user making the request
     * @param store          the target store
     * @return the {@link ValidationResult}
     */
    public ValidationResult validate(final OperationChain<?> operationChain, final User user, final Store store) {
        final ValidationResult validationResult = new ValidationResult();
        if (operationChain.getOperations().isEmpty()) {
            validationResult.addError("Operation chain contains no operations");
        } else {
            Class<? extends Output> output = null;
            for (final Operation op : operationChain.getOperations()) {
                output = validate(op, user, store, validationResult, output);
            }
        }

        return validationResult;
    }

    protected Class<? extends Output> validate(final Operation operation, final User user, final Store store, final ValidationResult validationResult, final Class<? extends Output> input) {
        validationResult.add(operation.validate());
        final Class<? extends Output> output = validateInputOutputTypes(operation, validationResult, store, input);
        validateViews(operation, user, store, validationResult);
        validateComparables(operation, user, store, validationResult);
        return output;
    }

    protected Class<? extends Output> validateInputOutputTypes(final Operation operation, final ValidationResult validationResult, final Store store, final Class<? extends Output> input) {
        Class<? extends Output> output = input;
        if (null == input) {
            if (operation instanceof Output) {
                output = ((Output) operation).getClass();
            }
        } else {
            final Operation firstOp = getFirstOperation(operation);
            if (firstOp instanceof Input) {
                final Class<?> outputType = OperationUtil.getOutputType(input);
                final Class<?> inputType = OperationUtil.getInputType(((Input) firstOp));

                validationResult.add(OperationUtil.isValid(outputType, inputType));
            } else {
                validationResult.addError("Invalid combination of operations: "
                        + input.getName() + " -> " + firstOp.getClass().getName()
                        + ". " + input.getClass().getSimpleName() + " has an output but "
                        + firstOp.getClass().getSimpleName() + " does not take an input.");
            }
            if (operation instanceof Output) {
                output = ((Output) operation).getClass();
            } else {
                output = null;
            }
        }
        return output;
    }

    protected Operation getFirstOperation(final Operation operation) {
        final Operation firstOp;
        if (operation instanceof OperationChain && !((OperationChain) operation).getOperations().isEmpty()) {
            firstOp = ((OperationChain<?>) operation).getOperations().get(0);
        } else {
            firstOp = operation;
        }
        return firstOp;
    }

    protected void validateComparables(final Operation op, final User user, final Store store, final ValidationResult validationResult) {
        if (op instanceof ElementComparison) {
            final Schema schema;
            try {
                schema = getSchema(op, user, store);
            } catch (final Exception e) {
                final String message = "Unable to getSchema to validate groups";
                LOGGER.info(message, e);
                validationResult.addError(message);
                return;
            }
            for (final Pair<String, String> pair : ((ElementComparison) op).getComparableGroupPropertyPairs()) {
                final SchemaElementDefinition elementDef = schema.getElement(pair.getFirst());
                if (null == elementDef) {
                    validationResult.addError(op.getClass().getName()
                            + " references " + pair.getFirst()
                            + " group that does not exist in the schema");
                } else {
                    Class<?> propertyClass = elementDef.getPropertyClass(pair.getSecond());
                    if (null != propertyClass && !Comparable.class.isAssignableFrom(propertyClass)) {
                        validationResult.addError("Property " + pair.getSecond()
                                + " in group " + pair.getFirst()
                                + " has a java class of " + propertyClass.getName()
                                + " which does not extend Comparable.");
                    }
                }
            }
        }
    }

    protected void validateViews(final Operation op, final User user, final Store store, final ValidationResult validationResult) {
        if (shouldValidate(op)) {
            final Schema schema;
            try {
                schema = getSchema(op, user, store);
                if (schema == null) {
                    throw new GafferRuntimeException(String.format("Schema was null with user=%s", user));
                }
            } catch (final Exception e) {
                final String message = "Unable to getSchema to validate groups";
                LOGGER.info(message, e);
                validationResult.addError(message);
                return;
            }
            final ValidationResult viewValidationResult = viewValidator.validate(getView(op), schema, getStoreTraits(store));
            if (!viewValidationResult.isValid()) {
                validationResult.addError("View for operation "
                        + op.getClass().getName()
                        + " is not valid. ");
                validationResult.add(viewValidationResult);
            }
        }
    }

    protected View getView(final Operation op) {
        return ((GraphFilters) op).getView();
    }

    protected boolean shouldValidate(final Operation op) {
        return op instanceof GraphFilters;
    }

    protected Schema getSchema(final Operation operation, final User user, final Store store) throws OperationException {
        return store.execute(new GetSchema.Builder().compact(true).build(), new Context(user));
    }

    protected Set<StoreTrait> getStoreTraits(final Store store) {
        return store.getTraits();
    }
}
