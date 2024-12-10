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

import uk.gov.gchq.gaffer.access.predicate.AccessPredicate;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Map;

@Since("2.4.0")
@Summary("Changes the access controls on a Graph")
public class ChangeGraphAccess implements Operation {

    private String graphId;
    private String owner;
    private Boolean isPublic;
    private AccessPredicate readPredicate;
    private AccessPredicate writePredicate;
    private Map<String, String> options;

    // Getters
    /**
     * Get the graph ID of the graph that will be changed.
     *
     * @return the graph ID
     */
    public String getGraphId() {
        return graphId;
    }

    public String getOwner() {
        return owner;
    }

    public Boolean isPublic() {
        return isPublic;
    }

    public AccessPredicate getReadPredicate() {
        return readPredicate;
    }

    public AccessPredicate getWritePredicate() {
        return writePredicate;
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

    public void setOwner(final String owner) {
        this.owner = owner;
    }

    public void setIsPublic(final Boolean isPublic) {
        this.isPublic = isPublic;
    }

    public void setReadPredicate(final AccessPredicate readPredicate) {
        this.readPredicate = readPredicate;
    }

    public void setWritePredicate(final AccessPredicate writePredicate) {
        this.writePredicate = writePredicate;
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
        return new ChangeGraphAccess.Builder()
                .graphId(graphId)
                .owner(owner)
                .isPublic(isPublic)
                .readPredicate(readPredicate)
                .writePredicate(writePredicate)
                .options(options)
                .build();
    }

    public static class Builder extends Operation.BaseBuilder<ChangeGraphAccess, Builder> {
        public Builder() {
            super(new ChangeGraphAccess());
        }

        /**
         * Set the current graph ID
         *
         * @param graphId the graph ID of the graph to alter
         * @return The builder
         */
        public Builder graphId(final String graphId) {
            _getOp().setGraphId(graphId);
            return _self();
        }

        /**
         * Set the new owner of the Graph.
         *
         * @param owner The owner.
         * @return The builder
         */
        public Builder owner(final String owner) {
            _getOp().setOwner(owner);
            return _self();
        }

        /**
         * Set if graph is public.
         *
         * @param isPublic Is the graph public.
         * @return The builder
         */
        public Builder isPublic(final Boolean isPublic) {
            _getOp().setIsPublic(isPublic);
            return _self();
        }

        /**
         * Set the read predicate for the graph.
         *
         * @param readPredicate The read predicate.
         * @return The builder.
         */
        public Builder readPredicate(final AccessPredicate readPredicate) {
            _getOp().setReadPredicate(readPredicate);
            return _self();
        }

        /**
         * Set the write predicate.
         *
         * @param writePredicate The write predicate.
         * @return The builder.
         */
        public Builder writePredicate(final AccessPredicate writePredicate) {
            _getOp().setWritePredicate(writePredicate);
            return _self();
        }
    }
}
