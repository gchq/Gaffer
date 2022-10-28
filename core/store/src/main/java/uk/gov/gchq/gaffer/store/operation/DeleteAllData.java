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

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.isNull;

/**
 * This operation is used to self delete all retained data
 */
@JsonPropertyOrder(value = {"class"}, alphabetic = true)
@Since("2.0.0")
@Summary("This operation is used to self delete all retained data")
public class DeleteAllData implements Operation {
    Map<String, String> options = new HashMap<>();

    @Override
    public DeleteAllData shallowClone() throws CloneFailedException {
        return new DeleteAllData();
    }

    @Override
    public Map<String, String> getOptions() {
        return isNull(options) ? Collections.emptyMap() : options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = new HashMap<>(options);
    }

    public static class Builder extends BaseBuilder<DeleteAllData, DeleteAllData.Builder> {
        public Builder() {
            super(new DeleteAllData());
        }
    }
}
