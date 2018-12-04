/*
 * Copyright 2018 Crown Copyright
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

package uk.gov.gchq.gaffer.mapstore.integration;

import uk.gov.gchq.gaffer.flink.integration.loader.AddElementsFromFileLoaderIT;
import uk.gov.gchq.gaffer.flink.integration.loader.AddElementsFromKafkaLoaderIT;
import uk.gov.gchq.gaffer.flink.integration.loader.AddElementsFromSocketLoaderIT;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;

import java.util.Arrays;

public class MapStoreFlinkITs extends MapStoreITs {
    private static final MapStoreProperties STORE_PROPERTIES =
            MapStoreProperties.loadStoreProperties("flinkstoreits.properties");

    public MapStoreFlinkITs() {
        super(STORE_PROPERTIES);
        singleTests(Arrays.asList(AddElementsFromFileLoaderIT.class,
                AddElementsFromKafkaLoaderIT.class,
                AddElementsFromSocketLoaderIT.class
        ));
    }
}
