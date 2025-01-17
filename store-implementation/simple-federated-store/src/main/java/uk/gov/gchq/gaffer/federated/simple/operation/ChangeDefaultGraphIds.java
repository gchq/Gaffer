/*
 * Copyright 2025 Crown Copyright
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

package uk.gov.gchq.gaffer.federated.simple.operation;

import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.List;
import java.util.Map;

@Since("2.4.0")
@Summary("Changes default Graph IDs of this store")
public class ChangeDefaultGraphIds implements Operation {

    private List<String> graphIds;
    private Map<String, String> options;

    // Getters
    /**
     * Get the new default graph IDs.
     *
     * @return the graph IDs
     */
    public List<String> geGraphIds() {
        return graphIds;
    }

    // Setters
    /**
     * Set the new default graph IDs.
     *
     * @param graphIds the graph IDs
     */
    public void setGraphIds(final List<String> graphIds) {
        this.graphIds = graphIds;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    @Override
    public Operation shallowClone() throws CloneFailedException {
        return new ChangeDefaultGraphIds.Builder()
                .graphIds(graphIds)
                .options(options)
                .build();
    }

    public static class Builder extends Operation.BaseBuilder<ChangeDefaultGraphIds, Builder> {
        public Builder() {
            super(new ChangeDefaultGraphIds());
        }

        /**
         * Set the current graph ID
         *
         * @param graphIds the graph ID to be changed
         * @return the builder
         */
        public Builder graphIds(final List<String> graphIds) {
            _getOp().setGraphIds(graphIds);
            return _self();
        }
    }

}
