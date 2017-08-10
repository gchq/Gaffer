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

package uk.gov.gchq.gaffer.graph.hook;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.impl.Limit;
import uk.gov.gchq.gaffer.operation.impl.export.set.ExportToSet;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.impl.output.ToArray;
import uk.gov.gchq.gaffer.operation.impl.output.ToList;
import uk.gov.gchq.gaffer.operation.impl.output.ToSet;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class GraphHooksTest {
    private static final String graphHooksPath = "graphHooks.json";
    private static final int RESULT_LIMIT = 100000;

    @Test
    public void shouldDeserialisationJson() throws IOException {
        // Given
        final GraphHook[] graphHooks = new JSONSerialiser().deserialise(StreamUtil.openStream(getClass(), graphHooksPath), GraphHook[].class);

        // Then
        assertEquals(1, graphHooks.length);
        final AddOperationsToChain addOperationsToChain = (AddOperationsToChain) graphHooks[0];
        for (final Class op : new Class[]{ToSet.class, ToArray.class, ToList.class, ExportToSet.class}) {
            assertEquals(RESULT_LIMIT, (int) ((Limit) addOperationsToChain.getBefore().get(op.getName()).get(0)).getResultLimit());
        }
        for (final Class op : new Class[]{GetElements.class, GetAllElements.class, GetAdjacentIds.class}) {
            assertEquals(RESULT_LIMIT, (int) ((Limit) addOperationsToChain.getAfter().get(op.getName()).get(0)).getResultLimit());
        }
    }
}
