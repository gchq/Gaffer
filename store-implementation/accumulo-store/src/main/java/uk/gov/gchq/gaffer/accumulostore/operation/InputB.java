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

package uk.gov.gchq.gaffer.accumulostore.operation;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import uk.gov.gchq.gaffer.operation.Operation;

public interface InputB<I> {
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    I getInputB();

    void setInputB(final I inputB);

    interface Builder<OP extends InputB<I>, I, B extends Builder<OP, I, ?>>
            extends Operation.Builder<OP, B> {
        default B inputB(final I inputB) {
            _getOp().setInputB(inputB);
            return _self();
        }
    }
}
