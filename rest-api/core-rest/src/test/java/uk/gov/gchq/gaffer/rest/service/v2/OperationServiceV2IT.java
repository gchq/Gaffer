/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.service.v2;

import org.junit.Test;

import uk.gov.gchq.gaffer.mapstore.operation.CountAllElementsDefaultView;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationChainDAO;
import uk.gov.gchq.gaffer.operation.impl.Count;
import uk.gov.gchq.gaffer.operation.impl.CountGroups;
import uk.gov.gchq.gaffer.operation.impl.DiscardOutput;
import uk.gov.gchq.gaffer.operation.impl.GetWalks;
import uk.gov.gchq.gaffer.operation.impl.If;
import uk.gov.gchq.gaffer.operation.impl.Limit;
import uk.gov.gchq.gaffer.operation.impl.Map;
import uk.gov.gchq.gaffer.operation.impl.Validate;
import uk.gov.gchq.gaffer.operation.impl.While;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.compare.Max;
import uk.gov.gchq.gaffer.operation.impl.compare.Min;
import uk.gov.gchq.gaffer.operation.impl.compare.Sort;
import uk.gov.gchq.gaffer.operation.impl.export.GetExports;
import uk.gov.gchq.gaffer.operation.impl.export.set.ExportToSet;
import uk.gov.gchq.gaffer.operation.impl.export.set.GetSetExport;
import uk.gov.gchq.gaffer.operation.impl.function.Aggregate;
import uk.gov.gchq.gaffer.operation.impl.function.Filter;
import uk.gov.gchq.gaffer.operation.impl.function.Transform;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.impl.output.ToArray;
import uk.gov.gchq.gaffer.operation.impl.output.ToCsv;
import uk.gov.gchq.gaffer.operation.impl.output.ToEntitySeeds;
import uk.gov.gchq.gaffer.operation.impl.output.ToList;
import uk.gov.gchq.gaffer.operation.impl.output.ToMap;
import uk.gov.gchq.gaffer.operation.impl.output.ToSet;
import uk.gov.gchq.gaffer.operation.impl.output.ToStream;
import uk.gov.gchq.gaffer.operation.impl.output.ToVertices;
import uk.gov.gchq.gaffer.rest.ServiceConstants;
import uk.gov.gchq.gaffer.rest.service.impl.OperationServiceIT;
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.operation.GetTraits;

import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class OperationServiceV2IT extends OperationServiceIT {

    @Test
    public void shouldReturnJobIdHeader() throws IOException {
        // When
        final Response response = client.executeOperation(new GetAllElements());

        // Then
        assertNotNull(response.getHeaderString(ServiceConstants.JOB_ID_HEADER));
    }

    @Test
    public void shouldReturnOperationDetailFieldsWithClass() throws IOException {
        // Given
        String expectedFields = "\"fields\":[{\"name\":\"input\",\"className\":\"java.lang.Object[]\",\"required\":false}," +
                "{\"name\":\"view\",\"className\":\"uk.gov.gchq.gaffer.data.elementdefinition.view.View\",\"required\":false}," +
                "{\"name\":\"includeIncomingOutGoing\",\"className\":\"java.lang.String\",\"required\":false}," +
                "{\"name\":\"seedMatching\",\"className\":\"java.lang.String\",\"required\":false}," +
                "{\"name\":\"options\",\"className\":\"java.util.Map<java.lang.String,java.lang.String>\",\"required\":false}," +
                "{\"name\":\"directedType\",\"className\":\"java.lang.String\",\"required\":false}," +
                "{\"name\":\"views\",\"className\":\"java.util.List<uk.gov.gchq.gaffer.data.elementdefinition.view.View>\",\"required\":false}]";

        // When
        Response response = client.getOperationDetails(GetElements.class);

        // Then
        assertTrue(response.readEntity(String.class).contains(expectedFields));
    }

    @Test
    public void shouldReturnAllOperationsAsOperationDetails() throws IOException {
        // Given
        final List<Class<? extends Operation>> expectedOperations = new ArrayList<>();
        expectedOperations.add(AddElements.class);
        expectedOperations.add(GetElements.class);
        expectedOperations.add(GetAdjacentIds.class);
        expectedOperations.add(GetAllElements.class);
        expectedOperations.add(ExportToSet.class);
        expectedOperations.add(GetSetExport.class);
        expectedOperations.add(GetExports.class);
        expectedOperations.add(ToArray.class);
        expectedOperations.add(ToEntitySeeds.class);
        expectedOperations.add(ToList.class);
        expectedOperations.add(ToMap.class);
        expectedOperations.add(ToCsv.class);
        expectedOperations.add(ToSet.class);
        expectedOperations.add(ToStream.class);
        expectedOperations.add(ToVertices.class);
        expectedOperations.add(Max.class);
        expectedOperations.add(Min.class);
        expectedOperations.add(Sort.class);
        expectedOperations.add(OperationChain.class);
        expectedOperations.add(OperationChainDAO.class);
        expectedOperations.add(GetWalks.class);
        expectedOperations.add(GenerateElements.class);
        expectedOperations.add(GenerateObjects.class);
        expectedOperations.add(Validate.class);
        expectedOperations.add(Count.class);
        expectedOperations.add(CountGroups.class);
        expectedOperations.add(Limit.class);
        expectedOperations.add(DiscardOutput.class);
        expectedOperations.add(GetSchema.class);
        expectedOperations.add(Map.class);
        expectedOperations.add(If.class);
        expectedOperations.add(While.class);
        expectedOperations.add(Filter.class);
        expectedOperations.add(Transform.class);
        expectedOperations.add(Aggregate.class);
        expectedOperations.add(GetTraits.class);
        expectedOperations.add(CountAllElementsDefaultView.class);

        // When
        Response response = getClient().getAllOperationsAsOperationDetails();
        List<OperationDetail> operationDetailList = response.readEntity(new GenericType<List<OperationDetail>>() {
        });

        // Then
        assertEquals(expectedOperations.size(), operationDetailList.size());

        // When
        List<String> operationDetailClassNameList = operationDetailList.stream()
                .map(operationDetail -> operationDetail.getName())
                .collect(Collectors.toList());

        List<String> expectedOperationClassNameList = expectedOperations.stream()
                .map(operationDetail -> operationDetail.getName())
                .collect(Collectors.toList());

        // Then
        assertTrue(operationDetailClassNameList.containsAll(expectedOperationClassNameList));
    }

    @Override
    protected RestApiV2TestClient getClient() {
        return new RestApiV2TestClient();
    }
}
