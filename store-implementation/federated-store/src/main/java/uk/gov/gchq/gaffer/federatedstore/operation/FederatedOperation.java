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

package uk.gov.gchq.gaffer.federatedstore.operation;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.Input;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.ValidationResult;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.DEFAULT_SKIP_FAILED_FEDERATED_EXECUTION;

/**
 * This operation federates a payload operation across a given set of graphs and merges the results with a given function.
 *
 * @param <INPUT>
 * @param <OUTPUT>
 */
@JsonPropertyOrder(value = {"class", "operation", "mergeFunction", "graphIds", "skipFailedFederatedExecution"}, alphabetic = true)
@Since("2.0.0")
@Summary("This operation federates a payload operation across a given set of graphs and merges the results with a given function.")
public class FederatedOperation<INPUT, OUTPUT> implements IFederationOperation, IFederatedOperation, Output<OUTPUT> {
    private String graphIdsCsv;
    @Required
    private Operation payloadOperation;
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "class")
    private Function<Iterable, OUTPUT> mergeFunction; //TODO FS Review change to Function<Iterable, OUTPUT>
    private boolean skipFailedFederatedExecution = DEFAULT_SKIP_FAILED_FEDERATED_EXECUTION;
    // TODO FS Feature, final boolean userRequestingAdminUsage = FederatedStoreUtil.isUserRequestingAdminUsage(operation);

    private Map<String, String> options;

    @JsonProperty("graphIds")
    public FederatedOperation graphIdsCSV(final String graphIds) {
        this.graphIdsCsv = graphIds;
        return this;
    }

    @JsonProperty("operation")
    public FederatedOperation payloadOperation(final Operation op) {
        if (this == op) {
            throw new GafferRuntimeException("Your attempting to add the FederatedOperation to its self as a payload, this will cause an infinite loop when cloned.");
        }
        this.payloadOperation = op;

        // TODO FS Examine, mergeOptions();

        return this;
    }

    public FederatedOperation mergeFunction(final Function<Iterable, OUTPUT> mergeFunction) {
        this.mergeFunction = mergeFunction;
        return this;
    }

    public boolean isSkipFailedFederatedExecution() {
        return skipFailedFederatedExecution;
    }

    public FederatedOperation<INPUT, OUTPUT> skipFailedFederatedExecution(final boolean skipFailedFederatedExecution) {
        this.skipFailedFederatedExecution = skipFailedFederatedExecution;
        return this;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
        // TODO FS Examine, mergeOptions();
    }

    @JsonProperty("graphIds")
    public String getGraphIdsCSV() {
        return graphIdsCsv;
    }

    @JsonIgnore
    public List<String> getGraphIds() {
        return FederatedStoreUtil.getCleanStrings(graphIdsCsv);
    }


    /**
     * Returns a shallow clone of the payload operation.
     *
     * @return cloned payload
     */
    @JsonProperty("operation")
    public Operation getPayloadOperation() {
        return Objects.isNull(payloadOperation) ? null : payloadOperation.shallowClone();
    }

    public Function<Iterable, OUTPUT> getMergeFunction() {
        return mergeFunction;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public FederatedOperation<INPUT, OUTPUT> shallowClone() throws CloneFailedException {
        try {
            return JSONSerialiser.deserialise(JSONSerialiser.serialise(this), FederatedOperation.class);
        } catch (SerialisationException e) {
            throw new CloneFailedException(e);
        }
    }


    @Override
    public boolean equals(final Object o) {
        final boolean rtn;
        if (this == o) {
            rtn = true;
        } else if (o == null || !(o instanceof FederatedOperation)) {
            rtn = false;
        } else {
            FederatedOperation that = (FederatedOperation) o;
            EqualsBuilder equalsBuilder = new EqualsBuilder()
                    .append(this.graphIdsCsv, that.graphIdsCsv)
                    .append(this.mergeFunction, that.mergeFunction)
                    .append(this.skipFailedFederatedExecution, that.skipFailedFederatedExecution)
                    .append(this.options, that.options);

            if (equalsBuilder.isEquals()) {
                try {
                    equalsBuilder.appendSuper(
                            this.payloadOperation.equals(that.payloadOperation)
                                    || Arrays.equals(
                                    JSONSerialiser.serialise(this.payloadOperation),
                                    JSONSerialiser.serialise(that.payloadOperation)));
                } catch (final SerialisationException e) {
                    throw new GafferRuntimeException("The operation to be federated could not be serialised to check equality", e);
                }
            }

            rtn = equalsBuilder.isEquals();
        }
        return rtn;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(11, 23)
                .append(graphIdsCsv)
                .append(payloadOperation)
                .append(mergeFunction)
                .append(skipFailedFederatedExecution)
                .append(options)
                .build();
    }

    @Override
    public TypeReference getOutputTypeReference() {
        return new TypeReferenceImpl.Object();
    }

    public static class Builder {
        private <INPUT, OUTPUT> BuilderParent<INPUT, OUTPUT> op(final InputOutput<INPUT, Object> op) {
            return new BuilderIO<>(op);
        }

        private <INPUT> BuilderParent<INPUT, Void> op(final Input<INPUT> op) {
            return new BuilderI<>(op);
        }

        private <OUTPUT> BuilderParent<Void, OUTPUT> op(final Output op) {
            return new BuilderO<>(op);
        }

        public <INPUT, OUTPUT> BuilderParent<INPUT, OUTPUT> op(Operation op) {
            BuilderParent rtn;
            if (op instanceof InputOutput) {
                rtn = op((InputOutput) op);
            } else if (op instanceof Input) {
                rtn = op((Input) op);
            } else if (op instanceof Output) {
                rtn = op((Output) op);
            } else {
                rtn = new BuilderNeitherIO(op);
            }
            return rtn;
        }

    }

    public static abstract class BuilderParent<INPUT, OUTPUT> extends BaseBuilder<FederatedOperation<INPUT, OUTPUT>, BuilderParent<INPUT, OUTPUT>> {
        public BuilderParent(FederatedOperation<INPUT, OUTPUT> fedOp) {
            super(fedOp);
        }

        public BuilderParent<INPUT, OUTPUT> graphIds(final String graphIds) {
            _getOp().graphIdsCSV(graphIds);
            return _self();
        }

        public BuilderParent<INPUT, OUTPUT> mergeFunction(final Function<Iterable, OUTPUT> mergeFunction) {
            _getOp().mergeFunction(mergeFunction);
            return _self();
        }

        public BuilderParent<INPUT, OUTPUT> skipFailedFederatedExecution(final boolean skipFailedFederatedExecution) {
            _getOp().skipFailedFederatedExecution(skipFailedFederatedExecution);
            return _self();
        }
    }

    private static class BuilderIO<INPUT, OUTPUT> extends FederatedOperation.BuilderParent<INPUT, OUTPUT> {
        private BuilderIO(InputOutput<INPUT, Object> op) {
            super(new FederatedOperation<>());
            FederatedOperation<INPUT, OUTPUT> fedOpIO = this._getOp();
            fedOpIO.payloadOperation(op);
        }
    }

    private static class BuilderI<INPUT> extends FederatedOperation.BuilderParent<INPUT, Void> {
        private BuilderI(Input<INPUT> op) {
            super(new FederatedOperation<>());
            FederatedOperation<INPUT, Void> fedOpI = this._getOp();
            fedOpI.payloadOperation(op);
        }
    }

    private static class BuilderO<OUTPUT> extends FederatedOperation.BuilderParent<Void, OUTPUT> {
        private BuilderO(Output op) {
            super(new FederatedOperation<>());
            FederatedOperation<Void, OUTPUT> fedOpO = this._getOp();
            fedOpO.payloadOperation(op);
        }
    }

    private static class BuilderNeitherIO extends FederatedOperation.BuilderParent<Void, Void> {
        private BuilderNeitherIO(Operation op) {
            super(new FederatedOperation<>());
            FederatedOperation<Void, Void> fedOpO = this._getOp();
            fedOpO.payloadOperation(op);
        }
    }

    @Override
    public void validateRequiredFieldPresent(final ValidationResult result, final Field field) {
        final Object value;
        try {
            value = field.get(this);
        } catch (final IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        //TODO FS Test, Prove this logic
        if (isNull(value) && (!field.getName().equals("mergeFunction") || isNull(payloadOperation) || payloadOperation instanceof Output)) {
            result.addError(field.getName() + " is required for: " + this.getClass().getSimpleName());
        }

        if (nonNull(value) && field.getName().equals("mergeFunction") && nonNull(payloadOperation) && !(payloadOperation instanceof Output)) {
            //TODO FS Examine, DO I want to error or just ignore and Log?
            result.addError(String.format("%s: %s is not required when payloadOperation: %s has no Output for: %s", field.getName(), mergeFunction.getClass().getSimpleName(), payloadOperation.getClass().getSimpleName(), this.getClass().getSimpleName()));
        }
    }
}
