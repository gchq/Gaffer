/*
 * Copyright 2024-2025 Crown Copyright
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

package uk.gov.gchq.gaffer.federated.simple.integration;

import org.junit.platform.suite.api.ConfigurationParameter;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.federated.simple.FederatedStoreProperties;
import uk.gov.gchq.gaffer.integration.AbstractStoreITs;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static uk.gov.gchq.gaffer.integration.junit.extensions.IntegrationTestSuiteExtension.INIT_CLASS;

/*
 * Implementation of the AbstractStoreITs with the simple federated store. This will initialise a pre-defined
 * Federated Store which already has two accmumlo-backed graphs added to it.
 */

@ConfigurationParameter(key = INIT_CLASS, value = "uk.gov.gchq.gaffer.federated.simple.integration.FederatedStoreITs")
public class FederatedStoreITs extends AbstractStoreITs {
    private static final FederatedStoreProperties STORE_PROPERTIES = FederatedStoreProperties
        .loadStoreProperties(StreamUtil.openStream(FederatedStoreITs.class, "store.properties"));

    private static final Schema SCHEMA = new Schema();
    private static final Map<String, String> TESTS_TO_SKIP = Stream.of(
            new SimpleEntry<>("shouldGetPathsWithSimpleGraphHook_2", "GetWalksIT - investigate"),
            new SimpleEntry<>("shouldGetEntityIds",
                "GetAdjacentIdsIT - Results duplicated due to returning EntityIds (Iterable) which will not dedup"),
            new SimpleEntry<>("shouldGetAllElementsWithFilterWithoutSummarisation",
                "GetAllElementsIT - count value is duplicated, elements correct otherwise"),
            new SimpleEntry<>("shouldImportFromFileThenCorrectlyExportToFile", "ImportExportCsvIT - Investigate further"),
            new SimpleEntry<>("shouldReturnDuplicateEdgesWhenNoAggregationIsUsed",
                "NoAggregationIT - Will return one from each graph as they contain the exact same elements"),
            new SimpleEntry<>("shouldReturnDuplicateEntitiesWhenNoAggregationIsUsed",
                "NoAggregationIT - Will return one from each graph as they contain the exact same elements"),
            new SimpleEntry<>("shouldAggregateOnlyRequiredGroupsWithQueryTimeAggregation", "PartAggregationIT - Investigate further"),
            new SimpleEntry<>("shouldAggregateOnlyRequiredGroups", "PartAggregationIT - Investigate further"),
            new SimpleEntry<>("shouldApplyPostOpAggregation", "SchemaMigrationIT - Need to apply schema aggregation choices"))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    FederatedStoreITs() {
        setSchema(SCHEMA);
        setStoreProperties(STORE_PROPERTIES);
        setTestsToSkip(TESTS_TO_SKIP);
    }
}
