/*
 * Copyright 2022 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation;

import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.operation.Operation;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.isNull;

public class DeleteAllDataOperation implements Operation {
    Map<String, String> options;

    @Override
    public DeleteAllDataOperation shallowClone() throws CloneFailedException {
        return new DeleteAllDataOperation();
    }

    @Override
    public Map<String, String> getOptions() {
        return isNull(options) ? Collections.emptyMap() : Collections.unmodifiableMap(options);
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = new HashMap<>(options);
    }

    public static class Builder extends BaseBuilder<DeleteAllDataOperation, DeleteAllDataOperation.Builder> {
        public Builder() {
            super(new DeleteAllDataOperation());
        }
    }
}
