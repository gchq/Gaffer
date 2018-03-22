/*
 * Copyright 2017-2018 Crown Copyright
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

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.Lists;

import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.operation.util.OperationUtil;

/**
 * {@code MultiEntityIdInput} operations are Gaffer operations which consume multiple
 * {@link EntityId}s.
 */
public interface MultiEntityIdInput extends MultiInput<EntityId> {
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "class")
    @JsonSetter("input")
    default void setInputFromVerticesAndIds(final Object[] input) {
        setInput(OperationUtil.toEntityIds(input));
    }

    @Override
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "class")
    default Object[] createInputArray() {
        return MultiInput.super.createInputArray();
    }

    @Override
    default void setInput(final EntityId[] input) {
        if (null == input) {
            setInput(((Iterable) null));
        }
        setInput(Lists.newArrayList(input));
    }

    interface Builder<OP extends MultiEntityIdInput, B extends Builder<OP, ?>>
            extends Input.Builder<OP, Iterable<? extends EntityId>, B> {
        @SuppressWarnings("unchecked")
        default B input(final Object... input) {
            if (null != _getOp().getInput()) {
                throw new IllegalStateException("Input has already been set");
            }
            return input(Lists.newArrayList(input));
        }

        @Override
        default B input(final Iterable input) {
            if (null != _getOp().getInput()) {
                throw new IllegalStateException("Input has already been set");
            }
            _getOp().setInput(OperationUtil.toEntityIds(input));
            return _self();
        }

        default B input(final EntityId... input) {
            if (null != _getOp().getInput()) {
                throw new IllegalStateException("Input has already been set");
            }
            return inputIds(Lists.newArrayList(input));
        }

        default B inputIds(final Iterable<? extends EntityId> input) {
            if (null != _getOp().getInput()) {
                throw new IllegalStateException("Input has already been set");
            }
            _getOp().setInput(input);
            return _self();
        }
    }
}
