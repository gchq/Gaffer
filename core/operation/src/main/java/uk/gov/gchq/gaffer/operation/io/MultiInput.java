/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.io;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * {@code MultiInput} operations are Gaffer operations which consume more than one
 * input.
 *
 * @param <I_ITEM> the type of input objects
 */
public interface MultiInput<I_ITEM> extends Input<Iterable<? extends I_ITEM>> {
    @SuppressFBWarnings(value = "PZLA_PREFER_ZERO_LENGTH_ARRAYS", justification = "If input is null then null should be returned")
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    @JsonGetter("input")
    default Object[] createInputArray() {
        return null != getInput() ? Iterables.toArray(getInput(), Object.class) : null;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    @JsonSetter("input")
    default void setInput(final I_ITEM[] input) {
        setInput(Lists.newArrayList(input));
    }

    interface Builder<OP extends MultiInput<I_ITEM>, I_ITEM, B extends Builder<OP, I_ITEM, ?>>
            extends Input.Builder<OP, Iterable<? extends I_ITEM>, B> {
        @SuppressWarnings("unchecked")
        default B input(final I_ITEM... input) {
            if (null != _getOp().getInput()) {
                throw new IllegalStateException("Input has already been set");
            }
            return input(Lists.newArrayList(input));
        }

        @Override
        default B input(final Iterable<? extends I_ITEM> input) {
            if (null != _getOp().getInput()) {
                throw new IllegalStateException("Input has already been set");
            }
            _getOp().setInput((Iterable) input);
            return _self();
        }
    }
}
