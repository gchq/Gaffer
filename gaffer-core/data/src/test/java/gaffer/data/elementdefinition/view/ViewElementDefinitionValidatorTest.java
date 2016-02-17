/*
 * Copyright 2016 Crown Copyright
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

package gaffer.data.elementdefinition.view;

import gaffer.commonutil.TestPropertyNames;
import gaffer.data.element.ElementComponentKey;
import gaffer.data.element.IdentifierType;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.element.function.ElementTransformer;
import gaffer.data.elementdefinition.TypedElementDefinitionValidatorTest;
import gaffer.function.TransformFunction;
import gaffer.function.context.ConsumerProducerFunctionContext;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ViewElementDefinitionValidatorTest extends TypedElementDefinitionValidatorTest {
    @Test
    public void shouldValidateAndReturnTrueWhenEmptyTransformer() {
        // Given
        final ViewElementDefinition elementDef = mock(ViewElementDefinition.class);
        final ViewElementDefinitionValidator validator = new ViewElementDefinitionValidator();
        given(elementDef.getIdentifiers()).willReturn(new HashSet<IdentifierType>());
        given(elementDef.getProperties()).willReturn(new HashSet<String>());
        given(elementDef.getFilter()).willReturn(mock(ElementFilter.class));
        given(elementDef.getTransformer()).willReturn(mock(ElementTransformer.class));

        // When
        final boolean isValid = validator.validate(elementDef);

        // Then
        assertTrue(isValid);
    }

    @Test
    public void shouldValidateAndReturnFalseWhenTransformerSelectionAMissingProperty() {
        // Given
        final ViewElementDefinition elementDef = mock(ViewElementDefinition.class);
        final ViewElementDefinitionValidator validator = new ViewElementDefinitionValidator();
        final ElementTransformer transformer = mock(ElementTransformer.class);
        final ConsumerProducerFunctionContext<ElementComponentKey, TransformFunction> context1 = mock(ConsumerProducerFunctionContext.class);
        final List<ConsumerProducerFunctionContext<ElementComponentKey, TransformFunction>> contexts = new ArrayList<>();
        contexts.add(context1);

        given(elementDef.getIdentifiers()).willReturn(new HashSet<IdentifierType>());
        given(elementDef.getProperties()).willReturn(new HashSet<>(Collections.singletonList(TestPropertyNames.F1)));
        given(elementDef.getFilter()).willReturn(mock(ElementFilter.class));
        given(elementDef.getTransformer()).willReturn(transformer);
        given(context1.getSelection()).willReturn(Collections.singletonList(new ElementComponentKey("missingProperty")));
        given(transformer.getFunctions()).willReturn(contexts);
        given(elementDef.getPropertyClass(TestPropertyNames.F1)).willReturn((Class) String.class);

        // When
        final boolean isValid = validator.validate(elementDef);

        // Then
        assertFalse(isValid);
    }

    @Test
    public void shouldValidateAndReturnFalseWhenTransformerProjectsToMissingProperty() {
        // Given
        final ViewElementDefinition elementDef = mock(ViewElementDefinition.class);
        final ViewElementDefinitionValidator validator = new ViewElementDefinitionValidator();
        final ElementTransformer transformer = mock(ElementTransformer.class);
        final ConsumerProducerFunctionContext<ElementComponentKey, TransformFunction> context1 = mock(ConsumerProducerFunctionContext.class);
        final List<ConsumerProducerFunctionContext<ElementComponentKey, TransformFunction>> contexts = new ArrayList<>();
        contexts.add(context1);

        given(elementDef.getIdentifiers()).willReturn(new HashSet<IdentifierType>());
        given(elementDef.getProperties()).willReturn(new HashSet<>(Collections.singletonList(TestPropertyNames.F1)));
        given(elementDef.getFilter()).willReturn(mock(ElementFilter.class));
        given(elementDef.getTransformer()).willReturn(transformer);
        given(context1.getProjection()).willReturn(Collections.singletonList(new ElementComponentKey("missingProperty")));
        given(transformer.getFunctions()).willReturn(contexts);
        given(elementDef.getPropertyClass(TestPropertyNames.F1)).willReturn((Class) String.class);

        // When
        final boolean isValid = validator.validate(elementDef);

        // Then
        assertFalse(isValid);
    }

    @Test
    public void shouldValidateAndReturnTrueWhenTransformerIsValid() {
        // Given
        final ViewElementDefinition elementDef = mock(ViewElementDefinition.class);
        final ViewElementDefinitionValidator validator = new ViewElementDefinitionValidator();
        final ElementTransformer transformer = mock(ElementTransformer.class);
        final ConsumerProducerFunctionContext<ElementComponentKey, TransformFunction> context1 = mock(ConsumerProducerFunctionContext.class);
        final TransformFunction function = mock(TransformFunction.class);
        final ElementComponentKey selectKey = new ElementComponentKey("selectionProperty");
        final ElementComponentKey projectKey = new ElementComponentKey("projectionProperty");

        final List<ConsumerProducerFunctionContext<ElementComponentKey, TransformFunction>> contexts = new ArrayList<>();
        contexts.add(context1);

        given(elementDef.getIdentifiers()).willReturn(new HashSet<IdentifierType>());
        given(elementDef.getProperties()).willReturn(new HashSet<>(Arrays.asList("selectionProperty", "projectionProperty")));
        given(elementDef.getFilter()).willReturn(mock(ElementFilter.class));
        given(elementDef.getTransformer()).willReturn(transformer);
        given(context1.getSelection()).willReturn(Collections.singletonList(selectKey));
        given(context1.getProjection()).willReturn(Collections.singletonList(projectKey));
        given(function.getInputClasses()).willReturn(new Class[]{String.class});
        given(function.getOutputClasses()).willReturn(new Class[]{Integer.class});
        given(context1.getFunction()).willReturn(function);
        given(transformer.getFunctions()).willReturn(contexts);
        given(elementDef.getClass(selectKey)).willReturn((Class) String.class);
        given(elementDef.getClass(projectKey)).willReturn((Class) Integer.class);
        given(elementDef.getPropertyClass("selectionProperty")).willReturn((Class) String.class);
        given(elementDef.getPropertyClass("projectionProperty")).willReturn((Class) Integer.class);

        // When
        final boolean isValid = validator.validate(elementDef);

        // Then
        assertTrue(isValid);
        verify(elementDef).getClass(selectKey);
        verify(function).getInputClasses();
    }

    @Override
    protected ViewElementDefinitionValidator newElementDefinitionValidator() {
        return new ViewElementDefinitionValidator();
    }
}