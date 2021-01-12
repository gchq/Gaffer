/*
 * Copyright 2016-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.integration.impl;

import uk.gov.gchq.gaffer.integration.GafferTest;
import uk.gov.gchq.gaffer.integration.MultiStoreTest;
import uk.gov.gchq.gaffer.integration.template.GetAdjacentIdsITTemplate;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;

@MultiStoreTest
@GafferTest(excludeStores = ParquetStore.class) // Parquet store doesn't implement GetAdjacentIds yet
public class GetAdjacentIdsIT extends GetAdjacentIdsITTemplate {

}