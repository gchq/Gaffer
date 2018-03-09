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

package uk.gov.gchq.gaffer.accumulostore.integration;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.MockAccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityKeyPackage;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.classic.ClassicKeyPackage;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsInRanges;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.user.User;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class GetElementsInRangesIT {
    @Test
    public void shouldReturnSameResultsFromByteEntityAndClassicKeyPackages() throws OperationException {
        // Given
        final Schema schema = new Schema.Builder()
                .edge("EDGE", new SchemaEdgeDefinition.Builder()
                        .source("string")
                        .destination("string")
                        .build())
                .type("string", String.class)
                .build();

        final AccumuloProperties propsByteEntity = new AccumuloProperties();
        propsByteEntity.setStoreClass(MockAccumuloStore.class);
        propsByteEntity.setKeyPackageClass(ByteEntityKeyPackage.class.getName());

        final AccumuloProperties propsClassic = new AccumuloProperties();
        propsClassic.setStoreClass(MockAccumuloStore.class);
        propsClassic.setKeyPackageClass(ClassicKeyPackage.class.getName());

        final Graph graphBE = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("byteEntity")
                        .build())
                .addSchema(schema)
                .storeProperties(propsByteEntity)
                .build();
        final Graph graphClassic = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("classic")
                        .build())
                .addSchema(schema)
                .storeProperties(propsClassic)
                .build();

        final List<Element> elements = Arrays.asList(
                new Edge.Builder()
                        .group("EDGE")
                        .source("A")
                        .dest("B")
                        .directed(true)
                        .build(),
                new Edge.Builder()
                        .group("EDGE")
                        .source("A")
                        .dest("C")
                        .directed(false)
                        .build()
        );

        graphBE.execute(new AddElements.Builder().input(elements).build(), new User());
        graphClassic.execute(new AddElements.Builder().input(elements).build(), new User());

        // Repeat test for all in/out values
        final Set<SeededGraphFilters.IncludeIncomingOutgoingType> inOutTypes = Sets.newHashSet(SeededGraphFilters.IncludeIncomingOutgoingType.values());
        inOutTypes.add(null);
        for (final SeededGraphFilters.IncludeIncomingOutgoingType inOutType : inOutTypes) {

            final GetElementsInRanges op = new GetElementsInRanges.Builder()
                    .input(Collections.singletonList(new Pair<>(new EdgeSeed("A", "A"), new EdgeSeed("A", "Z"))))
                    .inOutType(inOutType)
                    .build();

            // When
            final List<? extends Element> byteEntityResults = Lists.newArrayList(graphBE.execute(op, new User()));
            final List<? extends Element> classicResults = Lists.newArrayList(graphClassic.execute(op, new User()));

            // Then
            assertEquals(byteEntityResults.size(), classicResults.size());
            assertEquals(new HashSet<>(byteEntityResults), new HashSet<>(classicResults));
        }
    }
}
