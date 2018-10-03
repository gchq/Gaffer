/*
 * Copyright 2018 Crown Copyright
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

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.impl.join.match.Match;
import uk.gov.gchq.gaffer.operation.impl.join.match.MatchKey;
import uk.gov.gchq.gaffer.operation.impl.join.merge.Merge;
import uk.gov.gchq.gaffer.operation.impl.join.methods.JoinType;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Map;

/**
 * A {@code Join} Operation is used to join two Iterables together, specifying
 * a match and merge method.
 * <p>
 * Note: The input iterables are limited by default to 100,000 as these are read into memory as a Collection.
 * This limit can be changed by adding specifying a collectionLimit in the Operation.
 *
 * @param <I> Iterable input type.
 * @param <O> Iterable output type.
 */
@Since("1.7.0")
@Summary("Joins two iterables based on a join type")
@JsonPropertyOrder(value = {"input", "operation", "matchMethod", "matchKey", "mergeMethod", "joinType", "collectionLimit", "options"}, alphabetic = true)
public class Join<I, O> implements InputOutput<Iterable<? extends I>, Iterable<? extends O>>, MultiInput<I> {
    private Iterable<? extends I> leftSideInput;
    private Operation rightSideOperation;
    private Match matchMethod;
    private MatchKey matchKey;
    private Merge mergeMethod;
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

    public JoinType getJoinType() {
        return joinType;
    }

    public void setJoinType(final JoinType joinType) {
        this.joinType = joinType;
    }

    public Merge getMergeMethod() {
        return mergeMethod;
    }

    public void setMergeMethod(final Merge mergeMethod) {
        this.mergeMethod = mergeMethod;
    }

    public Integer getCollectionLimit() {
        return collectionLimit;
    }

    public void setCollectionLimit(final Integer collectionLimit) {
        this.collectionLimit = collectionLimit;
    }

    @Override
    public Join<I, O> shallowClone() throws CloneFailedException {
        return new Join.Builder<I, O>()
                .input(leftSideInput)
                .operation(rightSideOperation)
                .matchMethod(matchMethod)
                .matchKey(matchKey)
                .joinType(joinType)
                .mergeMethod(mergeMethod)
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
    public TypeReference<Iterable<? extends O>> getOutputTypeReference() {
        return TypeReferenceImpl.createIterableT();
    }

    public static final class Builder<I, O>
            extends BaseBuilder<Join<I, O>, Builder<I, O>>
            implements InputOutput.Builder<Join<I, O>,
            Iterable<? extends I>, Iterable<? extends O>,
            Builder<I, O>>,
            MultiInput.Builder<Join<I, O>, I, Builder<I, O>> {
        public Builder() {
            super(new Join<>());
        }

        public Builder<I, O> operation(final Operation operation) {
            _getOp().setOperation(operation);
            return _self();
        }

        public Builder<I, O> matchMethod(final Match matchMethod) {
            _getOp().setMatchMethod(matchMethod);
            return _self();
        }

        public Builder<I, O> joinType(final JoinType joinType) {
            _getOp().setJoinType(joinType);
            return _self();
        }

        public Builder<I, O> mergeMethod(final Merge mergeMethod) {
            _getOp().setMergeMethod(mergeMethod);
            return _self();
        }

        public Builder<I, O> matchKey(final MatchKey matchKey) {
            _getOp().setMatchKey(matchKey);
            return _self();
        }

        public Builder<I, O> collectionLimit(final Integer collectionLimit) {
            _getOp().setCollectionLimit(collectionLimit);
            return _self();
        }

    }
}
