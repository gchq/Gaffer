/*
 * Copyright 2022 Crown Copyright
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
package uk.gov.gchq.gaffer.store.operation;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DeleteAllDataTest extends OperationTest<DeleteAllDataOperation> {


    @Override
    public void builderShouldCreatePopulatedOperation() {
        final DeleteAllDataOperation operation = new DeleteAllDataOperation.Builder().option("a", "1").build();
        assertThat(operation.getOption("a")).isEqualTo(1);
    }

    @Override
    public void shouldShallowCloneOperation() {
        final DeleteAllDataOperation a = new DeleteAllDataOperation();
        final DeleteAllDataOperation b = a.shallowClone();
        assertThat(a).isNotSameAs(b).isEqualTo(b);
    }

    @Override
    protected DeleteAllDataOperation getTestObject() {
        return new DeleteAllDataOperation();
    }
}
