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

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import uk.gov.gchq.gaffer.commonutil.CloseableUtil;
import uk.gov.gchq.gaffer.operation.Operation;

import java.io.IOException;

/**
 * {@code Input} operations are any Gaffer operations which consume a single input.
 *
 * @param <I> the type of input object
 */
public interface Input<I> extends Operation {
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    I getInput();

    void setInput(final I input);

    @Override
    default void close() throws IOException {
        CloseableUtil.close(getInput());
    }

    interface Builder<OP extends Input<I>, I, B extends Builder<OP, I, ?>>
            extends Operation.Builder<OP, B> {
        default B input(final I input) {
            if (null != _getOp().getInput()) {
                throw new IllegalStateException("Input has already been set");
            }
            _getOp().setInput(input);
            return _self();
        }
    }
}
