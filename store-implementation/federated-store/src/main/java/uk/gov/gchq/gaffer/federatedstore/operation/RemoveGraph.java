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

package uk.gov.gchq.gaffer.federatedstore.operation;

import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.commonutil.Required;

import java.util.Map;

import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS;

/**
 * An Operation used for removing graphs from a FederatedStore.
 * <p>Requires:
 * <ul>
 * <li>graphId
 * </ul>
 * Does not delete the graph, just removes it from the scope of the FederatedStore.
 *
 * @see uk.gov.gchq.gaffer.federatedstore.FederatedStore
 * @see uk.gov.gchq.gaffer.graph.Graph
 */
public class RemoveGraph implements FederatedOperation {

    @Required
    private String graphId;
    private Map<String, String> options;

    public RemoveGraph() {
        addOption(KEY_OPERATION_OPTIONS_GRAPH_IDS, "");
    }

    public String getGraphId() {
        return graphId;
    }

    public void setGraphId(final String graphId) {
        this.graphId = graphId;
    }

    @Override
    public RemoveGraph shallowClone() throws CloneFailedException {
        return new RemoveGraph.Builder()
                .setGraphId(graphId)
                .options(options)
                .build();
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    public static class Builder extends BaseBuilder<RemoveGraph, Builder> {

        public Builder() {
            super(new RemoveGraph());
        }

        public Builder setGraphId(final String graphId) {
            _getOp().setGraphId(graphId);
            return _self();
        }
    }
}
