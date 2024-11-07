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

package uk.gov.gchq.gaffer.tinkerpop;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.tinkerpop.util.GafferPopTestUtil.StoreType;
import uk.gov.gchq.gaffer.tinkerpop.util.modern.GafferPopModernFederatedTestUtils;

/**
 * Test specific Federated features with Gremlin
 */
class GafferPopFederatedIT extends GafferPopFederationTests {

    @Override
    protected GafferPopGraph getGraph() throws OperationException {
        return GafferPopModernFederatedTestUtils.createModernGraph(GafferPopFederatedIT.class, StoreType.MAP);
    }
}
