/*
 * Copyright 2018-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.impl.join;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.Operations;
import uk.gov.gchq.gaffer.operation.impl.join.match.Match;
import uk.gov.gchq.gaffer.operation.impl.join.match.MatchKey;
import uk.gov.gchq.gaffer.operation.impl.join.methods.JoinType;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.tuple.MapTuple;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * A {@code Join} Operation is used to join two Iterables together, specifying
 * a match and merge method.
 * <p>
 * Note: The input iterables are limited by default to 100,000 as these are read into memory as a Collection.
 * This limit can be changed by adding specifying a collectionLimit in the Operation.
 *
 * @param <I> Iterable input type.
 */
@Since("1.8.0")
@Summary("Joins two iterables based on a join type")
@JsonPropertyOrder(value = {"input", "operation", "matchMethod", "matchKey", "flatten", "joinType", "collectionLimit", "options"}, alphabetic = true)
public class Join<I> implements InputOutput<Iterable<? extends I>,
        Iterable<? extends MapTuple>>, MultiInput<I>,
        Operations<Operation> {
    private Iterable<? extends I> leftSideInput;
    private Operation rightSideOperation;
    private Match matchMethod;
    private Boolean flatten = true;
    private MatchKey matchKey;
    private JoinType joinType;
    private Integer collectionLimit;
    private Map<String, String> options;

    @Override
    public Iterable<? extends I> getInput() {
        return leftSideInput;
    }

    @Override
    public void setInput(final Iterable<? extends I> leftSideInput) {
        this.leftSideInput = leftSideInput;
    }

    public Operation getOperation() {
        return rightSideOperation;
    }

    public void setOperation(final Operation rightSideOperation) {
        this.rightSideOperation = rightSideOperation;
    }

    public Match getMatchMethod() {
        return matchMethod;
    }

    public void setMatchMethod(final Match matchMethod) {
        this.matchMethod = matchMethod;
    }

    public MatchKey getMatchKey() {
        return matchKey;
    }

    public void setMatchKey(final MatchKey matchKey) {
        this.matchKey = matchKey;
    }

    public void setFlatten(final Boolean flatten) {
        this.flatten = flatten;
    }

    public Boolean isFlatten() {
        return flatten;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public void setJoinType(final JoinType joinType) {
        this.joinType = joinType;
    }

    public Integer getCollectionLimit() {
        return collectionLimit;
    }

    public void setCollectionLimit(final Integer collectionLimit) {
        this.collectionLimit = collectionLimit;
    }

    @Override
    public Join<I> shallowClone() throws CloneFailedException {
        return new Join.Builder<I>()
                .input(leftSideInput)
                .operation(rightSideOperation)
                .matchMethod(matchMethod)
                .matchKey(matchKey)
                .flatten(flatten)
                .joinType(joinType)
                .collectionLimit(collectionLimit)
                .options(options)
                .build();
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    @Override
    public TypeReference<Iterable<? extends MapTuple>> getOutputTypeReference() {
        return TypeReferenceImpl.createIterableT();
    }

    @JsonIgnore
    @Override
    public Collection<Operation> getOperations() {
        List operations = new ArrayList<>();
        operations.add(rightSideOperation);
        return operations;
    }

    public static final class Builder<I>
            extends BaseBuilder<Join<I>, Builder<I>>
            implements InputOutput.Builder<Join<I>,
            Iterable<? extends I>, Iterable<? extends MapTuple>,
            Builder<I>>,
            MultiInput.Builder<Join<I>, I, Builder<I>> {
        public Builder() {
            super(new Join<>());
        }

        public Builder<I> operation(final Operation operation) {
            _getOp().setOperation(operation);
            return _self();
        }

        public Builder<I> matchMethod(final Match matchMethod) {
            _getOp().setMatchMethod(matchMethod);
            return _self();
        }

        public Builder<I> flatten(final Boolean flatten) {
            _getOp().setFlatten(flatten);
            return _self();
        }

        public Builder<I> joinType(final JoinType joinType) {
            _getOp().setJoinType(joinType);
            return _self();
        }

        public Builder<I> matchKey(final MatchKey matchKey) {
            _getOp().setMatchKey(matchKey);
            return _self();
        }

        public Builder<I> collectionLimit(final Integer collectionLimit) {
            _getOp().setCollectionLimit(collectionLimit);
            return _self();
        }

    }
}
