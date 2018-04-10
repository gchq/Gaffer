/*
 * Copyright 2017-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.sparkaccumulo.integration.delete;

import org.apache.spark.api.java.JavaRDD;

import uk.gov.gchq.gaffer.accumulostore.integration.delete.AbstractDeletedElementsIT;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters.IncludeIncomingOutgoingType;
import uk.gov.gchq.gaffer.spark.operation.javardd.GetJavaRDDOfElements;

public class GetJavaRDDOfElementsDeletedElementsIT extends AbstractDeletedElementsIT<GetJavaRDDOfElements, JavaRDD<Element>> {
    @Override
    protected GetJavaRDDOfElements createGetOperation() {
        return new GetJavaRDDOfElements.Builder()
                .input(VERTICES)
                .inOutType(IncludeIncomingOutgoingType.OUTGOING)
                .build();
    }

    @Override
    protected void assertElements(final Iterable<ElementId> expected, final JavaRDD<Element> actual) {
        ElementUtil.assertElementEquals(expected, actual.collect());
    }
}
