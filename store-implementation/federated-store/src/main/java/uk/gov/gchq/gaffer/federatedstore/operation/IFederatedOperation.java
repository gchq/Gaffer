/*
 * Copyright 2021 Crown Copyright
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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.koryphe.Since;

import java.util.List;

/**
 * Interface for Operations that uses a selection graphs of graphs to be performed.
 */
@Since("2.0.0")
public interface IFederatedOperation extends Operation {

    @JsonProperty("graphIds")
    IFederatedOperation graphIdsCSV(final String graphIds);

    @JsonProperty("graphIds")
    String getGraphIdsCSV();

    @JsonIgnore
    default List<String> getGraphIds() {
        return FederatedStoreUtil.getCleanStrings(getGraphIdsCSV());
    }
}
