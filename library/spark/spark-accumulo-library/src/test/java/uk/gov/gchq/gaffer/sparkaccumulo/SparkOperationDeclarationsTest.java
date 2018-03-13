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

package uk.gov.gchq.gaffer.sparkaccumulo;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.spark.operation.dataframe.GetDataFrameOfElements;
import uk.gov.gchq.gaffer.spark.operation.graphframe.GetGraphFrameOfElements;
import uk.gov.gchq.gaffer.spark.operation.handler.graphframe.GetGraphFrameOfElementsHandler;
import uk.gov.gchq.gaffer.spark.operation.javardd.GetJavaRDDOfAllElements;
import uk.gov.gchq.gaffer.spark.operation.javardd.GetJavaRDDOfElements;
import uk.gov.gchq.gaffer.spark.operation.javardd.ImportJavaRDDOfElements;
import uk.gov.gchq.gaffer.spark.operation.scalardd.GetRDDOfAllElements;
import uk.gov.gchq.gaffer.spark.operation.scalardd.GetRDDOfElements;
import uk.gov.gchq.gaffer.spark.operation.scalardd.ImportRDDOfElements;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.dataframe.GetDataFrameOfElementsHandler;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.javardd.GetJavaRDDOfAllElementsHandler;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.javardd.GetJavaRDDOfElementsHandler;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.javardd.GetJavaRDDOfElementsInRangesHandler;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.javardd.ImportJavaRDDOfElementsHandler;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.javardd.ImportKeyValueJavaPairRDDToAccumuloHandler;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.scalardd.GetRDDOfAllElementsHandler;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.scalardd.GetRDDOfElementsHandler;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.scalardd.GetRDDOfElementsInRangesHandler;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.scalardd.ImportKeyValuePairRDDToAccumuloHandler;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.scalardd.ImportRDDOfElementsHandler;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.javardd.GetJavaRDDOfElementsInRanges;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.javardd.ImportKeyValueJavaPairRDDToAccumulo;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.scalardd.GetRDDOfElementsInRanges;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.scalardd.ImportKeyValuePairRDDToAccumulo;
import uk.gov.gchq.gaffer.store.operation.declaration.OperationDeclaration;
import uk.gov.gchq.gaffer.store.operation.declaration.OperationDeclaration.Builder;
import uk.gov.gchq.gaffer.store.operation.declaration.OperationDeclarations;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SparkOperationDeclarationsTest {
    public static final String ACCUMULO_OP_DECLARATIONS_JSON_PATH = "sparkAccumuloOperationsDeclarations.json";

    @Test
    public void shouldContainAllSparkOperationsAndHandlers() throws SerialisationException {
        // When
        final OperationDeclarations deserialised = JSONSerialiser
                .deserialise(StreamUtil.openStream(getClass(), ACCUMULO_OP_DECLARATIONS_JSON_PATH), OperationDeclarations.class);

        // Then
        final List<OperationDeclaration> deserialisedOps = deserialised.getOperations();
        final List<OperationDeclaration> expectedOps = Arrays.asList(
                new Builder()
                        .operation(GetJavaRDDOfElements.class)
                        .handler(new GetJavaRDDOfElementsHandler())
                        .build(),
                new Builder()
                        .operation(GetRDDOfElements.class)
                        .handler(new GetRDDOfElementsHandler())
                        .build(),
                new Builder()
                        .operation(GetRDDOfAllElements.class)
                        .handler(new GetRDDOfAllElementsHandler())
                        .build(),
                new Builder()
                        .operation(GetJavaRDDOfAllElements.class)
                        .handler(new GetJavaRDDOfAllElementsHandler())
                        .build(),
                new Builder()
                        .operation(GetDataFrameOfElements.class)
                        .handler(new GetDataFrameOfElementsHandler())
                        .build(),
                new Builder()
                        .operation(ImportKeyValueJavaPairRDDToAccumulo.class)
                        .handler(new ImportKeyValueJavaPairRDDToAccumuloHandler())
                        .build(),
                new Builder()
                        .operation(ImportJavaRDDOfElements.class)
                        .handler(new ImportJavaRDDOfElementsHandler())
                        .build(),
                new Builder()
                        .operation(ImportKeyValuePairRDDToAccumulo.class)
                        .handler(new ImportKeyValuePairRDDToAccumuloHandler())
                        .build(),
                new Builder()
                        .operation(ImportRDDOfElements.class)
                        .handler(new ImportRDDOfElementsHandler())
                        .build(),
                new Builder()
                        .operation(GetGraphFrameOfElements.class)
                        .handler(new GetGraphFrameOfElementsHandler())
                        .build(),
                new Builder()
                        .operation(GetJavaRDDOfElementsInRanges.class)
                        .handler(new GetJavaRDDOfElementsInRangesHandler())
                        .build(),
                new Builder()
                        .operation(GetRDDOfElementsInRanges.class)
                        .handler(new GetRDDOfElementsInRangesHandler())
                        .build()
        );
        assertEquals(expectedOps.size(), deserialisedOps.size());
        for (int i = 0; i < expectedOps.size(); i++) {
            assertEquals(expectedOps.get(i).getOperation(), deserialisedOps.get(i).getOperation());
            assertEquals(expectedOps.get(i).getHandler().getClass(), deserialisedOps.get(i).getHandler().getClass());
        }
    }
}
