/*
 * Copyright 2017-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.accumulostore.integration.delete;

import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsBetweenSetsPairs;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters.IncludeIncomingOutgoingType;

public class GetElementsBetweenSetsPairsDeletedElementsIT extends AbstractDeletedElementsIT<GetElementsBetweenSetsPairs, Iterable<? extends Element>> {

    @Override
    protected GetElementsBetweenSetsPairs createGetOperation() {
        return new GetElementsBetweenSetsPairs.Builder()
                .input((Object[]) VERTICES)
                .inputB((Object[]) VERTICES)
                .inOutType(IncludeIncomingOutgoingType.OUTGOING)
                .build();
    }
}
