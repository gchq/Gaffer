/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.federatedstore.integration;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties;
import uk.gov.gchq.gaffer.integration.AbstractStoreITs;
import uk.gov.gchq.gaffer.integration.impl.StoreValidationIT;
import uk.gov.gchq.gaffer.integration.impl.VisibilityIT;

public class FederatedStoreITs extends AbstractStoreITs {
    private static final FederatedStoreProperties STORE_PROPERTIES = FederatedStoreProperties.loadStoreProperties(
            StreamUtil.openStream(FederatedStoreITs.class, "predefinedFederatedStore.properties"));

    public FederatedStoreITs() {
        super(STORE_PROPERTIES);
        skipTest(VisibilityIT.class, "Visibility is not supported by the MapStore so the federated store doesn't support it when configured with a MapStore.");
        skipTest(StoreValidationIT.class, "StoreValidation is not supported by the MapStore so the federated store doesn't support it when configured with a MapStore.");
    }
}
