/*
 * Copyright 2017-2019 Crown Copyright
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

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Map;

import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants.KEY_OPERATION_OPTIONS_STORE_IDS;

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
@JsonPropertyOrder(value = {"class", "graphId"}, alphabetic = true)
@Since("1.0.0")
@Summary("Removes a Graph from the federated store")
public class RemoveStore implements FederatedOperation {

    @Required
    private String id;
    private Map<String, String> options;

    public RemoveStore() {
        addOption(KEY_OPERATION_OPTIONS_STORE_IDS, "");
    }

    public String getId() {
        return id;
    }

    public void setGraphId(final String graphId) {
        this.id = id;
    }

    @Override
    public RemoveStore shallowClone() throws CloneFailedException {
        return new RemoveStore.Builder()
                .id(id)
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

    public static class Builder extends BaseBuilder<RemoveStore, Builder> {

        public Builder() {
            super(new RemoveStore());
        }

        /**
         * Use {@link Builder#id} instead.
         *
         * @param id the store Id to set.
         * @return the builder
         */
        @Deprecated
        public Builder setId(final String id) {
            return id(id);
        }

        public Builder id(final String graphId) {
            _getOp().setGraphId(graphId);
            return _self();
        }
    }
}
