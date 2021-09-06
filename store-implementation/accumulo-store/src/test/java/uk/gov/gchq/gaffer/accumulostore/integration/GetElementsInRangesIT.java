/*
 * Copyright 2017-2021 Crown Copyright
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
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStoreTest;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityKeyPackage;
import uk.gov.gchq.gaffer.accumulostore.key.core.impl.classic.ClassicKeyPackage;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsInRanges;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
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

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.store.TestTypes.DIRECTED_EITHER;

public class GetElementsInRangesIT {
    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(AccumuloStoreTest.class));
    private static final AccumuloProperties CLASSIC_PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(AccumuloStoreTest.class, "/accumuloStoreClassicKeys.properties"));

    @Test
    public void shouldReturnSameResultsFromByteEntityAndClassicKeyPackages() throws OperationException {
        // Given
        final Schema schema = new Schema.Builder()
                .edge("EDGE", new SchemaEdgeDefinition.Builder()
                        .source("string")
                        .destination("string")
                        .directed(DIRECTED_EITHER)
                        .build())
                .type("string", String.class)
                .type(DIRECTED_EITHER, Boolean.class)
                .build();

        PROPERTIES.setKeyPackageClass(ByteEntityKeyPackage.class.getName());
        CLASSIC_PROPERTIES.setKeyPackageClass(ClassicKeyPackage.class.getName());

        final Graph graphBE = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("byteEntity")
                        .build())
                .addSchema(schema)
                .storeProperties(PROPERTIES)
                .build();
        final Graph graphClassic = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("classic")
                        .build())
                .addSchema(schema)
                .storeProperties(CLASSIC_PROPERTIES)
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
            assertThat(classicResults).hasSameSizeAs(byteEntityResults);
            assertThat(new HashSet<>(classicResults)).isEqualTo(new HashSet<>(byteEntityResults));
        }
    }
}
