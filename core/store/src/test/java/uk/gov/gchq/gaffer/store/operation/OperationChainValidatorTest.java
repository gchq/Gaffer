/*
 * Copyright 2016-2017 Crown Copyright
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

import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.Test;
import org.mockito.Mockito;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.DiscardOutput;
import uk.gov.gchq.gaffer.operation.impl.compare.Max;
import uk.gov.gchq.gaffer.operation.impl.export.GetExports;
import uk.gov.gchq.gaffer.operation.impl.export.set.ExportToSet;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.impl.output.ToVertices;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.ViewValidator;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.ValidationResult;
import java.util.Arrays;

public class OperationChainValidatorTest {
    @Test
    public void shouldValidateValidOperationChain() {
        validateOperationChain(new OperationChain(Arrays.asList(
                new GetElements(),
                new GetElements(),
                new ToVertices(),
                new GetAdjacentIds(),
                new Max()
        )), true);
    }

    @Test
    public void shouldValidateOperationChainThatCouldBeValidBasedOnGenerics() {
        validateOperationChain(new OperationChain(Arrays.asList(
                new GetElements(),
                new GetElements(),
                new ToVertices(),
                new GenerateObjects(),
                new GetAdjacentIds(),
                new Max()
        )), true);
    }

    @Test
    public void shouldValidateExportOperationChain() {
        validateOperationChain(new OperationChain(Arrays.asList(
                new GetElements(),
                new ExportToSet<>(),
                new DiscardOutput(),
                new GetElements(),
                new ExportToSet<>(),
                new DiscardOutput(),
                new GetExports()
        )), true);
    }

    @Test
    public void shouldValidateInvalidExportOperationChainWithoutDiscardOperation() {
        validateOperationChain(new OperationChain(Arrays.asList(
                new GetElements(),
                new ExportToSet<>(),
                // new DiscardOutput(),
                new GetElements(),
                new ExportToSet<>(),
                // new DiscardOutput(),
                new GetExports()   // No input
        )), false);
    }

    @Test
    public void shouldValidateInvalidOperationChainIterableNotAssignableFromMap() {
        validateOperationChain(new OperationChain(Arrays.asList(
                new GetElements(),
                new ExportToSet<>(),
                new DiscardOutput(),
                new GetElements(),
                new ExportToSet<>(),
                new DiscardOutput(),
                new GetExports(), // Output is a map
                new GetElements() // Input is an iterable
        )), false);
    }

    @Test
    public void shouldValidateInvalidOperationChain() {
        validateOperationChain(new OperationChain(Arrays.asList(
                new GetElements(),
                new GetElements(),
                new ToVertices(),
                new GetElements(),
                new Max(),          // Output is an Element
                new GetElements()   // Input is an Iterable
        )), false);
    }

    @Test
    public void shouldNotValidateInvalidOperationChain() {

        //Given
        Operation operation = Mockito.mock(Operation.class);
        given(operation.validate()).willReturn(new ValidationResult("SparkContext is required"));

        OperationChain opChain = new OperationChain(operation);

        // When
        validateOperationChain(opChain, false);

        // Then
        verify(operation).validate();
    }

    private void validateOperationChain(final OperationChain opChain, final boolean expectedResult) {
        // Given
        final ViewValidator viewValidator = mock(ViewValidator.class);
        final OperationChainValidator validator = new OperationChainValidator(viewValidator);
        final Store store = mock(Store.class);
        final User user = mock(User.class);


        given(viewValidator.validate(any(View.class), any(Schema.class), anyBoolean())).willReturn(new ValidationResult());


        // When
        final ValidationResult validationResult = validator.validate(opChain, user, store);

        // Then
        assertEquals(expectedResult, validationResult.isValid());
    }
}
