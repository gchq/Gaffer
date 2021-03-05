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
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.ValidationResult;
import uk.gov.gchq.koryphe.binaryoperator.KorypheBinaryOperator;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

/**
 * This operation federates a payload operation across a given set of graphs and merges the results with a given function.
 * @param <PAYLOAD> The operation to be federated and executed by delegate graphs
 */
@JsonPropertyOrder(value = {"class", "operation", "mergeFunction", "graphIds"}, alphabetic = true)
@Since("2.0.0")
@Summary("This operation federates a payload operation across a given set of graphs and merges the results with a given function.")
public class FederatedOperation<PAYLOAD extends Operation> implements IFederationOperation, IFederatedOperation, Output {
    private String graphIdsCsv;
    @Required
    private PAYLOAD payloadOperation;
    @Required
    private KorypheBinaryOperator mergeFunction;
    // TODO x final boolean userRequestingAdminUsage = FederatedStoreUtil.isUserRequestingAdminUsage(operation);

    private Map<String, String> options;

    @JsonProperty("graphIds")
    public FederatedOperation graphIdsCSV(final String graphIds) {
        this.graphIdsCsv = graphIds;
        return this;
    }

    @JsonProperty("operation")
    public FederatedOperation payloadOperation(final PAYLOAD op) {
        if (this == op) {
            throw new GafferRuntimeException("Your attempting to add the FederatedOperation to its self as a payload, this will cause an infinite loop when cloned.");
        }
        this.payloadOperation = op;

        // TODO x mergeOptions();

        return this;
    }

    public FederatedOperation mergeFunction(final KorypheBinaryOperator mergeFunction) {
        this.mergeFunction = mergeFunction;
        return this;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
        // TODO x mergeOptions();
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
    public PAYLOAD getPayloadOperation() {
        return Objects.isNull(payloadOperation) ? null : (PAYLOAD) payloadOperation.shallowClone();
    }

    public KorypheBinaryOperator getMergeFunction() {
        return mergeFunction;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public FederatedOperation<PAYLOAD> shallowClone() throws CloneFailedException {
        return new FederatedOperation.Builder<PAYLOAD>()
                .graphIds(graphIdsCsv)
                .op(getPayloadOperation()) /* shallow clone */
                .mergeFunction(mergeFunction)
                .options(options)
                .build();
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
                .append(options)
                .build();
    }

    @Override
    public TypeReference<Object> getOutputTypeReference() {
        return new TypeReferenceImpl.Object();
    }

    public static class Builder<OP extends Operation> extends BaseBuilder<FederatedOperation<OP>, Builder<OP>> {
        public Builder() {
            super(new FederatedOperation<>());
        }

        public Builder<OP> graphIds(final String graphIds) {
            _getOp().graphIdsCSV(graphIds);
            return _self();
        }

        public Builder<OP> op(final OP op) {
            _getOp().payloadOperation(op);
            return _self();
        }

        public Builder<OP> mergeFunction(final KorypheBinaryOperator mergeFunction) {
            _getOp().mergeFunction(mergeFunction);
            return _self();
        }

        public Builder<OP> options(final Map<String, String> options) {
            _getOp().setOptions(options);
            return _self();
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
        //TODO x Test Prove this logic
        if (isNull(value) && (!field.getName().equals("mergeFunction") || isNull(payloadOperation) || payloadOperation instanceof Output)) {
            result.addError(field.getName() + " is required for: " + this.getClass().getSimpleName());
        }

        if (nonNull(value) && field.getName().equals("mergeFunction") && nonNull(payloadOperation) && !(payloadOperation instanceof Output)) {
            //TODO X DO I want to error or just ignore and Log?
            result.addError(String.format("%s: %s is not required when payloadOperation: %s has no Output for: %s", field.getName(), mergeFunction.getClass().getSimpleName(), payloadOperation.getClass().getSimpleName(), this.getClass().getSimpleName()));
        }
    }
}
