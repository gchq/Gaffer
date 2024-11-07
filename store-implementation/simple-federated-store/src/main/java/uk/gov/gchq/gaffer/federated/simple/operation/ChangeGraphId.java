/*
 * Copyright 2024 Crown Copyright
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

import java.util.Map;

@Since("2.4.0")
@Summary("Changes Graph ID")
public class ChangeGraphId implements Operation {

    private String graphId;
    private String newGraphId;
    private Map<String, String> options;

    // Getters
    /**
     * Get the graph ID that will be changed.
     *
     * @return the graph ID
     */
    public String getGraphId() {
        return graphId;
    }

    /**
     * Get the new ID for the graph.
     *
     * @return the new graph ID
     */
    public String getNewGraphId() {
        return newGraphId;
    }

    // Setters
    /**
     * Set the graph ID of the current graph.
     *
     * @param graphId the graph ID
     */
    public void setGraphId(final String graphId) {
        this.graphId = graphId;
    }

     /**
     * Set the new graph ID of the current graph.
     *
     * @param newGraphId the new Graph ID
     */
    public void setNewGraphId(final String newGraphId) {
        this.newGraphId = newGraphId;
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
        return new ChangeGraphId.Builder()
            .graphId(graphId)
            .newGraphId(newGraphId)
            .options(options)
            .build();
    }

    public static class Builder extends Operation.BaseBuilder<ChangeGraphId, Builder> {
        public Builder() {
            super(new ChangeGraphId());
        }

        /**
         * Set the current graph ID
         *
         * @param graphId the graph ID to be changed
         * @return the builder
         */
        public Builder graphId(final String graphId) {
            _getOp().setGraphId(graphId);
            return _self();
        }

        /**
         * Set the new graph ID
         *
         * @param newGraphId the new graph ID
         * @return the builder
         */
        public Builder newGraphId(final String newGraphId) {
            _getOp().setNewGraphId(newGraphId);
            return _self();
        }
    }
}
