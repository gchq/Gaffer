/*
 * Copyright 2021-2024 Crown Copyright
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

import com.fasterxml.jackson.annotation.JsonProperty;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.koryphe.Since;

import java.util.List;

/**
 * Interface for Operations that uses a selection of graphs to be performed.
 */
@Since("2.0.0")
public interface IFederatedOperation extends Operation {

    @JsonProperty("graphIds")
    IFederatedOperation graphIds(final List<String> graphsIds);

    @JsonProperty("excludedGraphIds")
    IFederatedOperation excludedGraphIds(final List<String> excludedGraphsIds);

    IFederatedOperation graphIdsCSV(final String graphIds);
    IFederatedOperation excludedGraphIdsCSV(final String excludedGraphIds);

    @JsonProperty("graphIds")
    List<String> getGraphIds();

    @JsonProperty("excludedGraphIds")
    List<String> getExcludedGraphIds();
}
