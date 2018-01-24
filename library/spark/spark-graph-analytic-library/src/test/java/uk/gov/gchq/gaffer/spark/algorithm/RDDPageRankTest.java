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

import org.apache.spark.sql.SparkSession;
import org.junit.Ignore;
import org.junit.Test;
import scala.reflect.ClassTag$;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.spark.SparkSessionProvider;
import uk.gov.gchq.koryphe.ValidationResult;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RDDPageRankTest extends OperationTest<RDDPageRank> {

    private final SparkSession sparkSession = SparkSessionProvider.getSparkSession();

    @Test
    public void shouldInvalidateOperationIfMaxIterationsAndToleranceAreSet() {
        // Given
        final RDDPageRank op = new RDDPageRank.Builder()
                .input(sparkSession.sparkContext().emptyRDD(ClassTag$.MODULE$.apply(Element.class)))
                .maxIterations(1)
                .tolerance(0.1)
                .build();

        // Then
        assertFalse(op.validate().isValid());
    }

    @Test
    public void shouldInvalidateOperationIfMaxIterationsAndToleranceAreNotSet() {
        // Given
        final RDDPageRank op = new RDDPageRank.Builder()
                .input(sparkSession.sparkContext().emptyRDD(ClassTag$.MODULE$.apply(Element.class)))
                .build();

        // Then
        assertFalse(op.validate().isValid());
    }

    @Test
    public void shouldValidateOperation() {
        // Given
        final Operation op = getTestObject();

        // When
        final ValidationResult validationResult = op.validate();

        // Then
        assertTrue(validationResult.isValid());
    }

    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final RDDPageRank op = getTestObject();

        // Then
        assertThat(op.getInput(), is(notNullValue()));
        assertThat(op.getMaxIterations(), is(notNullValue()));
        assertThat(op.getMaxIterations(), is(equalTo(1)));
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final RDDPageRank op = getTestObject();

        // When
        final RDDPageRank clone = op.shallowClone();

        // Then
        assertThat(op, is(not(sameInstance(clone))));
        assertThat(op.getInput(), is(equalTo(clone.getInput())));
        assertThat(op.getTolerance(), is(equalTo(clone.getTolerance())));
        assertThat(op.getMaxIterations(), is(equalTo(clone.getMaxIterations())));
    }

    @Ignore
    @Override
    public void shouldJsonSerialiseAndDeserialise() {
        // Cannot easily JSON serialise an RDD - skip this test
    }

    @Override
    protected RDDPageRank getTestObject() {
        return new RDDPageRank.Builder()
                .input(sparkSession.sparkContext().emptyRDD(ClassTag$.MODULE$.apply(Element.class)))
                .maxIterations(1)
                .build();
    }
}
