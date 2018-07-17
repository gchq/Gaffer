/*
 * Copyright 2018 Crown Copyright
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

package uk.gov.gchq.gaffer.graph.hook.migrate;

import org.junit.Test;

import uk.gov.gchq.gaffer.JSONSerialisationTest;
import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.function.migration.ToInteger;
import uk.gov.gchq.gaffer.function.migration.ToLong;

public class MigrateElementTest extends JSONSerialisationTest<MigrateElement> {

    @Test
    public void shouldJsonSerialiseAndDeserialiseTwice() {
        // Given
        final MigrateElement testObject = getTestObject();

        // When
        final byte[] serialisedOnceJson = toJson(testObject);
        final MigrateElement deserialisedOnceTestObject = fromJson(serialisedOnceJson);
        final byte[] serialisedTwiceJson = toJson(deserialisedOnceTestObject);

        // Then
        JsonAssert.assertEquals(serialisedOnceJson, serialisedTwiceJson);
    }

    @Override
    protected MigrateElement getTestObject() {
        return new MigrateElement(
                MigrateElement.ElementType.ENTITY,
                "entityOld",
                "entityNew",
                new ElementTransformer.Builder()
                        .select("count")
                        .execute(new ToLong())
                        .project("count")
                        .build(),
                new ElementTransformer.Builder()
                        .select("count")
                        .execute(new ToInteger())
                        .project("count")
                        .build());
    }
}
