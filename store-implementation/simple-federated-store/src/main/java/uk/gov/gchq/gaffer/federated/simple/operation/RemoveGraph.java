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

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Map;

@Since("2.4.0")
@Summary("Removes a new Graph from the federated store optionally deleting all data")
@JsonPropertyOrder(value = { "class", "graphId" }, alphabetic = true)
public class RemoveGraph implements Operation {

    @Required
    private String graphId;
    private boolean deleteAllData = false;
    private Map<String, String> options;

    // Getters

    /**
     * Get the graph ID of the graph to remove.
     *
     * @return The graph ID.
     */
    public String getGraphId() {
        return graphId;
    }

    /**
     * Get whether to delete all the data.
     *
     * @return True if deleting all.
     */
    public boolean getDeleteAllData() {
        return deleteAllData;
    }

    // Setters

    /**
     * Set the graph ID of the graph to remove.
     *
     * @param graphId The graph ID.
     */
    public void setGraphId(final String graphId) {
        this.graphId = graphId;
    }

    /**
     * Set whether to delete all data as well.
     *
     * @param deleteAllData Delete all.
     */
    public void setDeleteAllData(final boolean deleteAllData) {
        this.deleteAllData = deleteAllData;
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
        return new RemoveGraph.Builder()
            .graphId(graphId)
            .deleteAllData(deleteAllData)
            .options(options)
            .build();
    }

    public static class Builder extends Operation.BaseBuilder<RemoveGraph, Builder> {
        public Builder() {
            super(new RemoveGraph());
        }

        /**
         * Set the graph ID.
         *
         * @param graphId The graph ID to set.
         * @return The builder.
         */
        public Builder graphId(final String graphId) {
            _getOp().setGraphId(graphId);
            return _self();
        }

        /**
         * Set if to delete all data from the graph.
         *
         * @param deleteAllData Delete all the data.
         * @return The builder.
         */
        public Builder deleteAllData(final boolean deleteAllData) {
            _getOp().setDeleteAllData(deleteAllData);
            return _self();
        }

    }

}
