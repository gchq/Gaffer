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

package uk.gov.gchq.gaffer.spark.algorithm;

import org.junit.Test;

import uk.gov.gchq.gaffer.algorithm.PageRank;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.spark.algorithm.handler.GraphFramePageRankHandler;
import uk.gov.gchq.gaffer.spark.algorithm.handler.IterableElementsPageRankHandler;
import uk.gov.gchq.gaffer.spark.algorithm.handler.JavaRDDPageRankHandler;
import uk.gov.gchq.gaffer.spark.algorithm.handler.RDDPageRankHandler;
import uk.gov.gchq.gaffer.store.operation.declaration.OperationDeclaration;
import uk.gov.gchq.gaffer.store.operation.declaration.OperationDeclarations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SparkOperationDeclarationsTest {
    public static final String OP_DECLARATIONS_JSON_PATH = "sparkOperationsDeclarations.json";

    @Test
    public void shouldContainAllSparkAnalyticOperationsAndHandlers() throws SerialisationException {
        // When
        final OperationDeclarations deserialised = JSONSerialiser
                .deserialise(StreamUtil.openStream(getClass(), OP_DECLARATIONS_JSON_PATH), OperationDeclarations.class);

        // Then
        assertEquals(5, deserialised.getOperations().size());

        final OperationDeclaration od0 = deserialised.getOperations().get(0);
        assertEquals(PageRank.class, od0.getOperation());
        assertTrue(od0.getHandler() instanceof GraphFramePageRankHandler);

        final OperationDeclaration od1 = deserialised.getOperations().get(1);
        assertEquals(GraphFramePageRank.class, od1.getOperation());
        assertTrue(od1.getHandler() instanceof GraphFramePageRankHandler);

        final OperationDeclaration od2 = deserialised.getOperations().get(2);
        assertEquals(IterableElementsPageRank.class, od2.getOperation());
        assertTrue(od2.getHandler() instanceof IterableElementsPageRankHandler);

        final OperationDeclaration od3 = deserialised.getOperations().get(3);
        assertEquals(RDDPageRank.class, od3.getOperation());
        assertTrue(od3.getHandler() instanceof RDDPageRankHandler);

        final OperationDeclaration od4 = deserialised.getOperations().get(4);
        assertEquals(JavaRDDPageRank.class, od4.getOperation());
        assertTrue(od4.getHandler() instanceof JavaRDDPageRankHandler);
    }
}
