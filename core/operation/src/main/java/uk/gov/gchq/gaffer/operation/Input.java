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

package uk.gov.gchq.gaffer.operation;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

public interface Input<I> extends Operation {
    /**
     * @return the operation input.
     */
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    I getInput();

    /**
     * @param input the operation input to be set.
     *              This can happen automatically from a previous operation if this operation is used in an
     *              {@link OperationChain}.
     */
    void setInput(final I input);

    interface Builder<OP extends Input<I>, I, B extends Builder<OP, I, ?>>
            extends Operation.Builder<OP, B> {
        default B input(final I input) {
            _getOp().setInput(input);
            return _self();
        }
    }
}
