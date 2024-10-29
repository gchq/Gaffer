/*
 * Copyright 2017-2024 Crown Copyright
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

import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

/**
 * @deprecated Will be removed in 2.4.0, functionality will be merged into the
 *             RemoveGraph operation.
 */
@Deprecated
@JsonPropertyOrder(value = {"class", "graphId"}, alphabetic = true)
@Since("2.0.0")
@Summary("Used to tell a graph to delete all data, before being removed.")
public class RemoveGraphAndDeleteAllData extends RemoveGraph {

    @Override
    public RemoveGraphAndDeleteAllData shallowClone() throws CloneFailedException {
        return new RemoveGraphAndDeleteAllData.Builder()
                .graphId(super.getGraphId())
                .options(super.getOptions())
                .setUserRequestingAdminUsage(super.isUserRequestingAdminUsage())
                .build();
    }

    public static class Builder extends IFederationOperation.BaseBuilder<RemoveGraphAndDeleteAllData, RemoveGraphAndDeleteAllData.Builder> {

        public Builder() {
            super(new RemoveGraphAndDeleteAllData());
        }

        public RemoveGraphAndDeleteAllData.Builder graphId(final String graphId) {
            _getOp().setGraphId(graphId);
            return _self();
        }
    }
}
