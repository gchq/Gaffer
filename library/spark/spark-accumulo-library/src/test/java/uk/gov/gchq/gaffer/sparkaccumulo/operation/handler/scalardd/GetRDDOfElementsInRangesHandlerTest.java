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
package uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.scalardd;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.rdd.RDD;

import uk.gov.gchq.gaffer.accumulostore.operation.handler.GetElementsInRangesHandlerTest;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters.IncludeIncomingOutgoingType;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.AbstractGetRDDHandler;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.scalardd.GetRDDOfElementsInRanges;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class GetRDDOfElementsInRangesHandlerTest extends GetElementsInRangesHandlerTest {
    private final String configurationString;

    public GetRDDOfElementsInRangesHandlerTest() {
        final Configuration configuration = new Configuration();
        try {
            configurationString = AbstractGetRDDHandler.convertConfigurationToString(configuration);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected OutputOperationHandler createHandler() {
        return new GetRDDOfElementsInRangesHandler();
    }

    @Override
    protected GetRDDOfElementsInRanges createOperation(final Set<Pair<ElementId, ElementId>> simpleEntityRanges, final View view, final IncludeIncomingOutgoingType inOutType, final DirectedType directedType) {
        return new GetRDDOfElementsInRanges.Builder()
                .input(simpleEntityRanges)
                .view(view)
                .directedType(directedType)
                .inOutType(inOutType)
                .option(AbstractGetRDDHandler.HADOOP_CONFIGURATION_KEY, configurationString)
                .build();
    }

    @Override
    protected List<Element> parseResults(final Object results) {
        return Arrays.asList((Element[]) ((RDD<Element>) results).collect());
    }
}
