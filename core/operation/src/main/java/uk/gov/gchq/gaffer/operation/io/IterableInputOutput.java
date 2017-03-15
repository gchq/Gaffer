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

package uk.gov.gchq.gaffer.operation.io;

import uk.gov.gchq.gaffer.operation.Operation;

public interface IterableInputOutput<I_ITEM, O> extends
        IterableInput<I_ITEM>,
        Output<O> {

    interface Builder<OP extends IterableInputOutput<I_ITEM, O>, I_ITEM, O, B extends Builder<OP, I_ITEM, O, ?>>
            extends Operation.Builder<OP, B>,
            IterableInput.Builder<OP, I_ITEM, B>,
            Output.Builder<OP, O, B> {
    }
}
