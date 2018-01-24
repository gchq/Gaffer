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

package uk.gov.gchq.gaffer.spark.function;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;
import org.graphframes.examples.Graphs$;
import org.junit.Test;

import uk.gov.gchq.gaffer.spark.SparkContextUtil;
import uk.gov.gchq.gaffer.spark.SparkSessionProvider;

import java.util.List;
import java.util.function.Function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

public class GraphFrameToIterableRowTest {

    @Test
    public void shouldConvertGraphFrameToIterableOfRows() {
        // Given
        final SparkSession sparkSession = SparkSessionProvider.getSparkSession();

        final Function<GraphFrame, Iterable<? extends Row>> function = new GraphFrameToIterableRow();

        final GraphFrame graphFrame = Graphs$.MODULE$.friends();

        // When
        final Iterable<? extends Row> result = function.apply(graphFrame);
        final List<Row> resultList = Lists.newArrayList(result);

        // Then
        assertThat(resultList, hasSize(15));
    }
}
