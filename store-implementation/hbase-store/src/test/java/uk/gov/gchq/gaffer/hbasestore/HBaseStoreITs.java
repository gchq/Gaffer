/*
 * Copyright 2016 Crown Copyright
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
package uk.gov.gchq.gaffer.hbasestore;

import org.junit.Ignore;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.integration.AbstractStoreITs;
import uk.gov.gchq.gaffer.integration.impl.AggregationIT;
import uk.gov.gchq.gaffer.integration.impl.CountGroupsIT;
import uk.gov.gchq.gaffer.integration.impl.ExportIT;
import uk.gov.gchq.gaffer.integration.impl.GetAdjacentEntitySeedsIT;
import uk.gov.gchq.gaffer.integration.impl.GetElementsIT;
import uk.gov.gchq.gaffer.integration.impl.StoreValidationIT;
import uk.gov.gchq.gaffer.integration.impl.TransformationIT;
import uk.gov.gchq.gaffer.store.StoreProperties;

@Ignore
public class HBaseStoreITs extends AbstractStoreITs {
    private static final StoreProperties STORE_PROPERTIES = StoreProperties.loadStoreProperties(StreamUtil.storeProps(HBaseStoreITs.class));

    public HBaseStoreITs() {
        super(STORE_PROPERTIES);
        skipTest(ExportIT.class, "Not implemented yet");
        skipTest(GetAdjacentEntitySeedsIT.class, "Not implemented yet");
        skipTest(TransformationIT.class, "Not implemented yet");
        skipTest(GetElementsIT.class, "Not implemented yet");
        skipTest(GetElementsIT.class, "Not implemented yet");
        skipTest(CountGroupsIT.class, "Not implemented yet");
        skipTest(AggregationIT.class, "Not implemented yet");
        skipTest(StoreValidationIT.class, "Not implemented yet");
    }
}
