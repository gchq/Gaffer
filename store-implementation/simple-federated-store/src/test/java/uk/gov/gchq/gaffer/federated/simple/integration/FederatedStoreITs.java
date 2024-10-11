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

package uk.gov.gchq.gaffer.federated.simple.integration;

import static uk.gov.gchq.gaffer.integration.junit.extensions.IntegrationTestSuiteExtension.INIT_CLASS;

import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.platform.suite.api.ConfigurationParameter;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.federated.simple.FederatedStoreProperties;
import uk.gov.gchq.gaffer.integration.AbstractStoreITs;
import uk.gov.gchq.gaffer.store.schema.Schema;

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
            new SimpleEntry<>("shouldGetPathsWithSimpleGraphHook_1", "Unable to add Hooks to individual fed graphs"),
            new SimpleEntry<>("shouldGetPathsWithSimpleGraphHook_2", "Unable to add Hooks to individual fed graphs"),
            new SimpleEntry<>("shouldExecuteForEachOperationOnGetElementsWithValidResults", "ForEachIT - Investigate further"),
            new SimpleEntry<>("shouldExecuteForEachOperationOnGetElementsWithEmptyIterable", "ForEachIT - Investigate further"),
            new SimpleEntry<>("shouldGetEntityIds", "GetAdjacentIdsIT - Investigate further"),
            new SimpleEntry<>("shouldConvertToDomainObjects", "GeneratorsIT - Investigate further"),
            new SimpleEntry<>("shouldGetAllElementsWithFilterWithoutSummarisation", "GetAllElementsIT - Investigate further"),
            new SimpleEntry<>("shouldImportFromFileThenCorrectlyExportToFile", "ImportExportCsvIT - Investigate further"),
            new SimpleEntry<>("shouldRightSideFullJoinUsingKeyFunctionMatch", "JoinIT - Investigate further"),
            new SimpleEntry<>("shouldReturnDuplicateEdgesWhenNoAggregationIsUsed", "NoAggregationIT - Investigate further"),
            new SimpleEntry<>("shouldReturnDuplicateEntitiesWhenNoAggregationIsUsed", "NoAggregationIT - Investigate further"),
            new SimpleEntry<>("shouldAggregateOnlyRequiredGroupsWithQueryTimeAggregation", "PartAggregationIT - Investigate further"),
            new SimpleEntry<>("shouldAggregateOnlyRequiredGroups", "PartAggregationIT - Investigate further"),
            new SimpleEntry<>("shouldApplyPostOpAggregation", "SchemaMigrationIT - Investigate further"),
            new SimpleEntry<>("shouldResolveNamedViewWithinNamedOperation", "GraphHooksIT - Can add and get NamedOps/Views but cannot run them"),
            new SimpleEntry<>("shouldRepeatedlyAddElements", "WhileIT - Investigate further"))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    FederatedStoreITs() {
        setSchema(SCHEMA);
        setStoreProperties(STORE_PROPERTIES);
        setTestsToSkip(TESTS_TO_SKIP);
    }
}
