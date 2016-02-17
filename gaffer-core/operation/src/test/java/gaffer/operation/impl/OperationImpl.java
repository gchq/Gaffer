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

package gaffer.operation.impl;

import gaffer.data.elementdefinition.view.View;
import gaffer.operation.AbstractOperation;
import gaffer.operation.Operation;

public class OperationImpl<INPUT, OUTPUT> extends AbstractOperation<INPUT, OUTPUT> {
    public OperationImpl() {
    }

    public OperationImpl(final INPUT input) {
        super(input);
    }

    public OperationImpl(final View view) {
        super(view);
    }

    public OperationImpl(final View view, final INPUT input) {
        super(view, input);
    }

    public OperationImpl(final Operation<INPUT, ?> operation) {
        super(operation);
    }
}