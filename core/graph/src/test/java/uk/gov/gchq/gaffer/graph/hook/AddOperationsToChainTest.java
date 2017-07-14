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
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.Count;
import uk.gov.gchq.gaffer.operation.impl.CountGroups;
import uk.gov.gchq.gaffer.operation.impl.DiscardOutput;
import uk.gov.gchq.gaffer.operation.impl.Limit;
import uk.gov.gchq.gaffer.operation.impl.SplitStore;
import uk.gov.gchq.gaffer.operation.impl.Validate;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class AddOperationsToChainTest {
    public static final String ADD_OPERATIONS_TO_CHAIN_PATH = "src/test/resources/addOperationsToChain.json";

    @Test
    public void shouldAddAllOperations() throws IOException {
        AddOperationsToChain addOperationsToChain = new AddOperationsToChain(ADD_OPERATIONS_TO_CHAIN_PATH);

        Operation discardOutput = mock(DiscardOutput.class);
        Operation splitStore = mock(SplitStore.class);
        Operation validate = mock(Validate.class);
        Operation getAdjacentIds = mock(GetAdjacentIds.class);
        Operation count = mock(Count.class);
        Operation countGroups = mock(CountGroups.class);
        Operation getElements = mock(GetElements.class);
        Operation getAllElements = mock(GetAllElements.class);
        Operation limit = mock(Limit.class);


        final List expectedOperations = new ArrayList<Operation>();
        expectedOperations.add(discardOutput);
        expectedOperations.add(splitStore);
        expectedOperations.add(validate);
        expectedOperations.add(getAdjacentIds);
        expectedOperations.add(count);
        expectedOperations.add(discardOutput);
        expectedOperations.add(countGroups);
        expectedOperations.add(getElements);
        expectedOperations.add(getAllElements);
        expectedOperations.add(limit);
        expectedOperations.add(validate);
        expectedOperations.add(count);

        final OperationChain opChain = new OperationChain.Builder()
                .first(getAdjacentIds)
                .then(getElements)
                .then(getAllElements)
                .build();

        addOperationsToChain.preExecute(opChain, null);

        for (int i = 0; i > opChain.getOperations().size(); i++) {
            assertEquals(expectedOperations.get(i).getClass().getName(), opChain.getOperations().get(i).getClass().getName());
        }
    }
}
