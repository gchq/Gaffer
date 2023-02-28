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

import uk.gov.gchq.gaffer.operation.OperationTest;

import static org.assertj.core.api.Assertions.assertThat;

public class DeleteAllDataTest extends OperationTest<DeleteAllData> {


    @Override
    public void builderShouldCreatePopulatedOperation() {
        final DeleteAllData operation = new DeleteAllData.Builder().option("a", "1").build();
        assertThat(operation.getOption("a")).isEqualTo(1);
    }

    @Override
    public void shouldShallowCloneOperationREVIEWMAYBEDELETE() {
        final DeleteAllData a = new DeleteAllData();
        final DeleteAllData b = a.shallowClone();
        assertThat(a).isNotSameAs(b).isEqualTo(b);
    }

    @Override
    protected DeleteAllData getTestObjectOld() {
        return new DeleteAllData();
    }
}
