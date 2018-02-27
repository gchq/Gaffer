/*
 * Copyright 2017 Crown Copyright
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

import org.apache.commons.lang3.exception.CloneFailedException;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class TestOperationsImpl implements Operation, Operations<Operation> {

    private final List<Operation> ops;

    public TestOperationsImpl(final List<Operation> ops) {
        this.ops = ops;
    }

    @Override
    public Operation shallowClone() throws CloneFailedException {
        return null;
    }

    @Override
    public Map<String, String> getOptions() {
        return null;
    }

    @Override
    public void setOptions(final Map<String, String> options) {

    }

    @Override
    public Collection<Operation> getOperations() {
        return ops;
    }
}
