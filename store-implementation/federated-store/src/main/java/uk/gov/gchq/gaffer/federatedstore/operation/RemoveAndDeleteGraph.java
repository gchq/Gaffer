/*
 * Copyright 2017-2022 Crown Copyright
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


@JsonPropertyOrder(value = {"class", "graphId"}, alphabetic = true)
public class RemoveAndDeleteGraph extends RemoveGraph {

    @Override
    public RemoveAndDeleteGraph shallowClone() throws CloneFailedException {
        return new RemoveAndDeleteGraph.Builder()
                .graphId(super.getGraphId())
                .options(super.getOptions())
                .setUserRequestingAdminUsage(super.isUserRequestingAdminUsage())
                .build();
    }

    public static class Builder extends IFederationOperation.BaseBuilder<RemoveAndDeleteGraph, RemoveAndDeleteGraph.Builder> {

        public Builder() {
            super(new RemoveAndDeleteGraph());
        }

        public RemoveAndDeleteGraph.Builder graphId(final String graphId) {
            _getOp().setGraphId(graphId);
            return _self();
        }
    }
}
