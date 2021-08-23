/*
 * Copyright 2016-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation.handler;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "class")
@JsonPropertyOrder(value = {"class"}, alphabetic = true)
public interface OperationHandler<O> {

    String OPERATION_DID_NOT_CONTAIN_REQUIRED_FIELDS = "Operation did not contain required fields. [";
    String FIELD_S_OF_TYPE_S = "Field:%s of Type:%s, ";
    String SUMMARY = "Summary";
    String FIELD_WITH_FIRST_UPPERCASE = "Field name defined in Handler started with uppercase case, bad aesthetics when FieldDeclaration is JSONSerialised. field: %s";

    default O doOperation(final Operation operation, final Context context, final Store store) throws OperationException {
        try {


            validateOperation(operation, getFieldDeclaration());
            return _doOperation(operation, context, store);

        } catch (final Exception e) {
            throw new OperationException(String.format("Error with Executor: %s handling operation: %s with handler: %s due to: %s", store.getGraphId(), operation.getId(), this.getClass().getCanonicalName(), e.getMessage()), e);
        }
    }

    public static void validateOperation(final Operation operation, final FieldDeclaration fieldDeclaration) {
        final List<String> collect = getOperationErrorsForIncorrectValueType(operation, fieldDeclaration);

        if (!collect.isEmpty()) {
            final StringBuilder errorMessage = new StringBuilder().append(OPERATION_DID_NOT_CONTAIN_REQUIRED_FIELDS);
            collect.forEach(errorMessage::append);
            errorMessage.append(" ]");
            throw new IllegalArgumentException(errorMessage.toString());
        }
    }

    static List<String> getOperationErrorsForIncorrectValueType(final Operation operation, final FieldDeclaration fieldDeclaration) {
        final TreeMap<String, Class> fieldDeclarations = fieldDeclaration.getFields();
        final List<String> rtn = fieldDeclarations.entrySet().stream()
                .filter(e -> {
                    final String key = e.getKey();
                    final boolean noKeyFoundInOperation = !operation.containsKey(key);
                    final boolean keyIsNotOptional = !fieldDeclaration.optionalContains(key); //case insensitive
                    final boolean noCompulsoryKeyFound = noKeyFoundInOperation && keyIsNotOptional;
                    final boolean iskeyInvalid;
                    if (noCompulsoryKeyFound) {
                        iskeyInvalid = true;
                    } else {
                        final Object value = operation.get(key);
                        //hasValueNotExpectedType
                        iskeyInvalid = nonNull(value) && !e.getValue().isInstance(value);
                    }
                    return iskeyInvalid;
                })
                .map(e -> String.format(FIELD_S_OF_TYPE_S, e.getKey(), e.getValue().getCanonicalName()))
                .collect(Collectors.toList());

        final List<String> fieldsWithCapitals = fieldDeclarations.keySet().stream().filter(s -> Character.isUpperCase(s.charAt(0)))
                .map(s -> String.format(FIELD_WITH_FIRST_UPPERCASE, s))
                .collect(Collectors.toList());

        rtn.addAll(fieldsWithCapitals);

        return rtn;
    }

    static List<String> getOperationErrorsForNullAndIncorrectValueType(final Operation operation, final FieldDeclaration fieldDeclaration) {
        return fieldDeclaration.getFields().entrySet().stream()
                .filter(e -> {
                    final String key = e.getKey();
                    final boolean noKeyFoundInOperation = !operation.containsKey(key);
                    final boolean keyIsNotOptional = !fieldDeclaration.optionalContains(key); //case insensitive
                    final boolean noCompulsoryKeyFound = noKeyFoundInOperation && keyIsNotOptional;
                    final boolean isKeyInvalid;
                    if (noCompulsoryKeyFound) {
                        isKeyInvalid = true;
                    } else {
                        final Object value = operation.get(key);
                        //noValueOrNotExpectedType
                        isKeyInvalid = isNull(value) || !e.getValue().isInstance(value);
                    }
                    return isKeyInvalid;
                })
                .map(e -> String.format(FIELD_S_OF_TYPE_S, e.getKey(), e.getValue().getCanonicalName())).collect(Collectors.toList());
    }

    O _doOperation(Operation operation, Context context, Store store) throws OperationException;

    @JsonIgnore
    FieldDeclaration getFieldDeclaration();

    @JsonProperty("fieldDeclaration")
    default FieldDeclaration jsonNonEmptyFieldDeclaration() {
        final FieldDeclaration fieldDeclaration = getFieldDeclaration();
        return fieldDeclaration.getFields().isEmpty() ? null : fieldDeclaration;
    }


    abstract class AbstractBuilder<B extends AbstractBuilder> {
        protected Operation operation;

        abstract protected B getBuilder();

        public B id(String id) {
            this.operation = new Operation(id);
            return getBuilder();
        }

        public B operationArg(final String operationArg, final Object value) {
            operation.operationArg(operationArg, value);
            return getBuilder();
        }

        public B operationArgs(final Map<String, Object> operationsArgs) throws NullPointerException {
            operation.operationArgs(operationsArgs);
            return getBuilder();
        }

        public Operation build() {
            if (isNull(operation.getId())) {
                throw new IllegalArgumentException("Operation requires ID");
            }

            return operation;
        }
    }

    class BuilderGenericOperation extends AbstractBuilder<BuilderGenericOperation> {
        @Override
        protected BuilderGenericOperation getBuilder() {
            return this;
        }
    }

    abstract class BuilderSpecificOperation<B extends AbstractBuilder> extends AbstractBuilder<B> {
        abstract protected FieldDeclaration getFieldDeclaration();

        @Override
        public Operation build() {
            Operation op = super.build();
            OperationHandler.validateOperation(op, getFieldDeclaration());
            return op;
        }
    }

    abstract class BuilderSpecificInputOperation<B extends AbstractBuilder> extends BuilderSpecificOperation<B> {
        public B input(Object input) {
            operationArg("input", input);
            return getBuilder();
        }
    }

}
