/*
 * Copyright 2016 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.graph.GraphFilters;
import uk.gov.gchq.gaffer.operation.impl.compare.ElementComparison;
import uk.gov.gchq.gaffer.operation.io.Input;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import uk.gov.gchq.gaffer.store.schema.ViewValidator;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.ValidationResult;

/**
 * Validation class for validating {@link OperationChain}s against {@link ViewValidator}s.
 */
public class OperationChainValidator {
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
            final Schema schema = store.getSchema();
            Class<? extends Output> output = null;
            for (final Operation op : operationChain.getOperations()) {
                validationResult.add(op.validate());
                output = validateInputOutputTypes(op, validationResult, store, output);
                validateViews(op, validationResult, schema, store);
                validateComparables(op, validationResult, schema, store);
            }
        }

        return validationResult;
    }

    protected Class<? extends Output> validateInputOutputTypes(final Operation operation, final ValidationResult validationResult, final Store store, final Class<? extends Output> output) {
        Class<? extends Output> newOutput = output;
        if (null == output) {
            if (operation instanceof Output) {
                newOutput = ((Output) operation).getClass();
            }
        } else {
            if (operation instanceof Input) {
                final Class<?> outputType = OperationUtil.getOutputType(output);
                final Class<?> inputType = OperationUtil.getInputType(((Input) operation));

                validationResult.add(OperationUtil.isValid(outputType, inputType));
            } else {
                validationResult.addError("Invalid combination of operations: "
                        + output.getName() + " -> " + operation.getClass().getName()
                        + ". " + output.getClass().getSimpleName() + " has an output but "
                        + operation.getClass().getSimpleName() + " does not take an input.");
            }
            if (operation instanceof Output) {
                newOutput = ((Output) operation).getClass();
            } else {
                newOutput = null;
            }
        }
        return newOutput;
    }

    protected void validateComparables(final Operation op, final ValidationResult validationResult, final Schema schema, final Store store) {
        if (op instanceof ElementComparison) {
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

    protected void validateViews(final Operation op, final ValidationResult validationResult, final Schema schema, final Store store) {
        final View opView;
        if (op instanceof GraphFilters) {
            opView = ((GraphFilters) op).getView();
        } else {
            opView = null;
        }

        final ValidationResult viewValidationResult = viewValidator.validate(opView, schema, store.getTraits());
        if (!viewValidationResult.isValid()) {
            validationResult.addError("View for operation "
                    + op.getClass().getName()
                    + " is not valid. ");
            validationResult.add(viewValidationResult);
        }
    }
}
