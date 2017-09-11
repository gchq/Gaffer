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

package uk.gov.gchq.gaffer.store.schema;

import com.google.common.collect.Sets;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.function.ExampleAggregateFunction;
import uk.gov.gchq.gaffer.function.ExampleFilterFunction;
import uk.gov.gchq.gaffer.function.ExampleTuple2BinaryOperator;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;
import uk.gov.gchq.koryphe.impl.predicate.Exists;
import uk.gov.gchq.koryphe.impl.predicate.IsA;
import uk.gov.gchq.koryphe.impl.predicate.IsXMoreThanY;

import java.util.Collections;
import java.util.Date;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public abstract class SchemaElementDefinitionTest<T extends SchemaElementDefinition> {
    public static final String PROPERTY_STRING_TYPE = "property.string";

    protected abstract SchemaElementDefinition.BaseBuilder<T, ?> createBuilder();

    protected abstract SchemaElementDefinition.BaseBuilder<T, ?> createEmptyBuilder();

    @Test
    public void shouldNotBeAbleToAddPropertiesOnceBuilt() {
        // Given
        final T elementDef = createBuilder().build();

        // When / Then
        try {
            elementDef.getPropertyMap()
                    .put("new property", "string");
            fail("Exception expected");
        } catch (final UnsupportedOperationException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void shouldNotBeAbleToAddIdentifiersOnceBuilt() {
        // Given
        final T elementDef = createBuilder().build();

        // When / Then
        try {
            elementDef.getIdentifierMap()
                    .put(IdentifierType.SOURCE, "string");
            fail("Exception expected");
        } catch (final UnsupportedOperationException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void shouldNotBeAbleToModifyGroupByOnceBuilt() {
        // Given
        final T elementDef = createBuilder()
                .property("property", PROPERTY_STRING_TYPE)
                .build();

        // When / Then
        try {
            elementDef.getGroupBy().add("property");
            fail("Exception expected");
        } catch (final UnsupportedOperationException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void shouldNotBeAbleToModifyParentsOnceBuilt() {
        // Given
        final T elementDef = createBuilder()
                .property("property", PROPERTY_STRING_TYPE)
                .parents("parentGroup1")
                .build();

        // When / Then
        try {
            elementDef.getParents().add("parentGroup2");
            fail("Exception expected");
        } catch (final UnsupportedOperationException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void shouldReturnValidatorWithNoFunctionsWhenNoVerticesOrProperties() {
        // Given
        final T elementDef = createEmptyBuilder().build();

        // When
        final ElementFilter validator = elementDef.getValidator();

        // Then
        assertEquals(0, validator.getComponents().size());
        // Check the validator is cached
        assertSame(validator, elementDef.getValidator());
    }

    @Test
    public void shouldReturnFullValidator() {
        // Given
        final T elementDef = createBuilder()
                .property("property", PROPERTY_STRING_TYPE)
                .property("property1", PROPERTY_STRING_TYPE)
                .property("property2", PROPERTY_STRING_TYPE)
                .validator(new ElementFilter.Builder()
                        .select("property1", "property2")
                        .execute(new IsXMoreThanY())
                        .build())
                .build();

        setupSchema(elementDef);

        // When
        final ElementFilter validator = elementDef.getValidator();

        // Then
        int i = 0;
        assertEquals(IsXMoreThanY.class, validator.getComponents().get(i).getPredicate().getClass());
        assertArrayEquals(new String[]{"property1", "property2"},
                validator.getComponents().get(i).getSelection());
        i++;
        if (elementDef instanceof SchemaEdgeDefinition) {
            assertEquals(Integer.class.getName(), ((IsA) validator.getComponents().get(i).getPredicate()).getType());
            assertArrayEquals(new String[]{IdentifierType.SOURCE.name()},
                    validator.getComponents().get(i).getSelection());
            i++;
            assertEquals(Date.class.getName(), ((IsA) validator.getComponents().get(i).getPredicate()).getType());
            assertArrayEquals(new String[]{IdentifierType.DESTINATION.name()},
                    validator.getComponents().get(i).getSelection());
            i++;
            assertEquals(Boolean.class.getName(), ((IsA) validator.getComponents().get(i).getPredicate()).getType());
            assertArrayEquals(new String[]{IdentifierType.DIRECTED.name()},
                    validator.getComponents().get(i).getSelection());
            i++;
        } else {
            assertArrayEquals(new String[]{IdentifierType.VERTEX.name()},
                    validator.getComponents().get(i).getSelection());
            i++;
        }
        assertEquals(String.class.getName(), ((IsA) validator.getComponents().get(i).getPredicate()).getType());
        assertArrayEquals(new String[]{"property"},
                validator.getComponents().get(i).getSelection());
        i++;
        assertEquals(Exists.class, validator.getComponents().get(i).getPredicate().getClass());
        assertArrayEquals(new String[]{"property"},
                validator.getComponents().get(i).getSelection());
        i++;
        assertEquals(String.class.getName(), ((IsA) validator.getComponents().get(i).getPredicate()).getType());
        assertArrayEquals(new String[]{"property1"},
                validator.getComponents().get(i).getSelection());
        i++;
        assertEquals(Exists.class, validator.getComponents().get(i).getPredicate().getClass());
        assertArrayEquals(new String[]{"property1"},
                validator.getComponents().get(i).getSelection());
        i++;
        assertEquals(String.class.getName(), ((IsA) validator.getComponents().get(i).getPredicate()).getType());
        assertArrayEquals(new String[]{"property2"},
                validator.getComponents().get(i).getSelection());
        i++;
        assertEquals(Exists.class, validator.getComponents().get(i).getPredicate().getClass());
        assertArrayEquals(new String[]{"property2"},
                validator.getComponents().get(i).getSelection());
        i++;
        assertEquals(i, validator.getComponents().size());
        // Check the validator is cached
        assertSame(validator, elementDef.getValidator());
        assertNotSame(validator, elementDef.getValidator(false));
    }

    @Test
    public void shouldReturnFullValidatorWithoutIsA() {
        // Given
        final T elementDef = createBuilder()
                .property("property", PROPERTY_STRING_TYPE)
                .property("property1", PROPERTY_STRING_TYPE)
                .property("property2", PROPERTY_STRING_TYPE)
                .validator(new ElementFilter.Builder()
                        .select("property1", "property2")
                        .execute(new IsXMoreThanY())
                        .build())
                .build();

        setupSchema(elementDef);

        // When
        final ElementFilter validator = elementDef.getValidator(false);

        // Then
        int i = 0;
        assertEquals(IsXMoreThanY.class, validator.getComponents().get(i).getPredicate().getClass());
        assertArrayEquals(new String[]{"property1", "property2"},
                validator.getComponents().get(i).getSelection());
        i++;
        assertEquals(Exists.class, validator.getComponents().get(i).getPredicate().getClass());
        assertArrayEquals(new String[]{"property"},
                validator.getComponents().get(i).getSelection());
        i++;
        assertEquals(Exists.class, validator.getComponents().get(i).getPredicate().getClass());
        assertArrayEquals(new String[]{"property1"},
                validator.getComponents().get(i).getSelection());
        i++;
        assertEquals(Exists.class, validator.getComponents().get(i).getPredicate().getClass());
        assertArrayEquals(new String[]{"property2"},
                validator.getComponents().get(i).getSelection());
        i++;
        assertEquals(i, validator.getComponents().size());

        // Check the validator is cached
        // Check the validator is cached
        assertSame(validator, elementDef.getValidator(false));
        assertNotSame(validator, elementDef.getValidator(true));
    }


    @Test
    public void shouldBuildElementDefinition() {
        // Given
        final ElementFilter validator = mock(ElementFilter.class);

        // When
        final T elementDef = createBuilder()
                .property(TestPropertyNames.PROP_1, "property.integer")
                .property(TestPropertyNames.PROP_2, "property.object")
                .validator(validator)
                .build();
        setupSchema(elementDef);

        // Then
        assertEquals(2, elementDef.getProperties().size());
        assertEquals(Integer.class, elementDef.getPropertyClass(TestPropertyNames.PROP_1));
        assertEquals(Object.class, elementDef.getPropertyClass(TestPropertyNames.PROP_2));
        assertSame(validator, elementDef.getOriginalValidator());
    }

    @Test
    public void shouldReturnFullAggregator() {
        // Given
        final T elementDef = createBuilder()
                .property("property1", PROPERTY_STRING_TYPE)
                .property("property2", PROPERTY_STRING_TYPE)
                .property("property3", PROPERTY_STRING_TYPE)
                .property("visibility", PROPERTY_STRING_TYPE)
                .property("timestamp", PROPERTY_STRING_TYPE)
                .groupBy("property1", "property2")
                .build();

        setupSchema(elementDef);

        // When
        final ElementAggregator aggregator = elementDef.getFullAggregator();

        // Then
        assertEquals(5, aggregator.getComponents().size());
        assertTrue(aggregator.getComponents().get(0).getBinaryOperator() instanceof ExampleAggregateFunction);
        assertArrayEquals(new String[]{"property1"},
                aggregator.getComponents().get(0).getSelection());
        assertTrue(aggregator.getComponents().get(1).getBinaryOperator() instanceof ExampleAggregateFunction);
        assertArrayEquals(new String[]{"property2"},
                aggregator.getComponents().get(1).getSelection());
        assertTrue(aggregator.getComponents().get(2).getBinaryOperator() instanceof ExampleAggregateFunction);
        assertArrayEquals(new String[]{"property3"},
                aggregator.getComponents().get(2).getSelection());
        assertTrue(aggregator.getComponents().get(3).getBinaryOperator() instanceof ExampleAggregateFunction);
        assertArrayEquals(new String[]{"visibility"},
                aggregator.getComponents().get(3).getSelection());
        assertTrue(aggregator.getComponents().get(4).getBinaryOperator() instanceof ExampleAggregateFunction);
        assertArrayEquals(new String[]{"timestamp"},
                aggregator.getComponents().get(4).getSelection());
        // Check the aggregator is locked.
        try {
            aggregator.getComponents().add(null);
            fail("Exception expected");
        } catch (final UnsupportedOperationException e) {
            assertNotNull(e);
        }
        // check the aggregator is cached
        assertSame(aggregator, elementDef.getFullAggregator());
    }

    @Test
    public void shouldReturnFullAggregatorWithMultiPropertyAggregator() {
        // Given
        final T elementDef = createBuilder()
                .property("property1", PROPERTY_STRING_TYPE)
                .property("property2", PROPERTY_STRING_TYPE)
                .property("property3", PROPERTY_STRING_TYPE)
                .property("property4", PROPERTY_STRING_TYPE)
                .property("property5", PROPERTY_STRING_TYPE)
                .property("visibility", PROPERTY_STRING_TYPE)
                .property("timestamp", PROPERTY_STRING_TYPE)
                .groupBy("property1", "property2")
                .aggregator(new ElementAggregator.Builder()
                        .select("property1", "property2")
                        .execute(new ExampleTuple2BinaryOperator())
                        .build())
                .build();

        setupSchema(elementDef);

        // When
        final ElementAggregator aggregator = elementDef.getFullAggregator();

        // Then
        assertEquals(6, aggregator.getComponents().size());
        assertTrue(aggregator.getComponents().get(0).getBinaryOperator() instanceof ExampleTuple2BinaryOperator);
        assertArrayEquals(new String[]{"property1", "property2"},
                aggregator.getComponents().get(0).getSelection());

        assertTrue(aggregator.getComponents().get(1).getBinaryOperator() instanceof ExampleAggregateFunction);
        assertArrayEquals(new String[]{"property3"},
                aggregator.getComponents().get(1).getSelection());

        assertTrue(aggregator.getComponents().get(2).getBinaryOperator() instanceof ExampleAggregateFunction);
        assertArrayEquals(new String[]{"property4"},
                aggregator.getComponents().get(2).getSelection());

        assertTrue(aggregator.getComponents().get(3).getBinaryOperator() instanceof ExampleAggregateFunction);
        assertArrayEquals(new String[]{"property5"},
                aggregator.getComponents().get(3).getSelection());

        assertTrue(aggregator.getComponents().get(4).getBinaryOperator() instanceof ExampleAggregateFunction);
        assertArrayEquals(new String[]{"visibility"},
                aggregator.getComponents().get(4).getSelection());

        assertTrue(aggregator.getComponents().get(5).getBinaryOperator() instanceof ExampleAggregateFunction);
        assertArrayEquals(new String[]{"timestamp"},
                aggregator.getComponents().get(5).getSelection());
        // Check the aggregator is locked.
        try {
            aggregator.getComponents().add(null);
            fail("Exception expected");
        } catch (final UnsupportedOperationException e) {
            assertNotNull(e);
        }
        // check the aggregator is cached
        assertSame(aggregator, elementDef.getFullAggregator());
    }

    @Test
    public void shouldReturnIngestAggregator() {
        // Given
        final T elementDef = createBuilder()
                .property("property1", PROPERTY_STRING_TYPE)
                .property("property2", PROPERTY_STRING_TYPE)
                .property("property3", PROPERTY_STRING_TYPE)
                .property("visibility", PROPERTY_STRING_TYPE)
                .property("timestamp", PROPERTY_STRING_TYPE)
                .groupBy("property1", "property2")
                .build();

        setupSchema(elementDef);

        // When
        final ElementAggregator aggregator = elementDef.getIngestAggregator();

        // Then
        assertEquals(2, aggregator.getComponents().size());
        assertTrue(aggregator.getComponents().get(0).getBinaryOperator() instanceof ExampleAggregateFunction);
        assertArrayEquals(new String[]{"property3"},
                aggregator.getComponents().get(0).getSelection());
        assertTrue(aggregator.getComponents().get(1).getBinaryOperator() instanceof ExampleAggregateFunction);
        assertArrayEquals(new String[]{"timestamp"},
                aggregator.getComponents().get(1).getSelection());
        // Check the aggregator is locked.
        try {
            aggregator.getComponents().add(null);
            fail("Exception expected");
        } catch (final UnsupportedOperationException e) {
            assertNotNull(e);
        }
        // check the aggregator is cached
        assertSame(aggregator, elementDef.getIngestAggregator());
    }

    @Test
    public void shouldReturnIngestAggregatorWithMultiPropertyAggregator() {
        // Given
        final T elementDef = createBuilder()
                .property("property1", PROPERTY_STRING_TYPE)
                .property("property2", PROPERTY_STRING_TYPE)
                .property("property3", PROPERTY_STRING_TYPE)
                .property("property4", PROPERTY_STRING_TYPE)
                .property("property5", PROPERTY_STRING_TYPE)
                .property("visibility", PROPERTY_STRING_TYPE)
                .property("timestamp", PROPERTY_STRING_TYPE)
                .groupBy("property1", "property2")
                .aggregator(new ElementAggregator.Builder()
                        .select("property1", "property2")
                        .execute(new ExampleTuple2BinaryOperator())
                        .select("property3", "timestamp")
                        .execute(new ExampleTuple2BinaryOperator())
                        .build())
                .build();

        setupSchema(elementDef);

        // When
        final ElementAggregator aggregator = elementDef.getIngestAggregator();

        // Then
        int i = 0;
        assertTrue(aggregator.getComponents().get(i).getBinaryOperator() instanceof ExampleTuple2BinaryOperator);
        assertArrayEquals(new String[]{"property3", "timestamp"},
                aggregator.getComponents().get(i).getSelection());
        i++;
        assertTrue(aggregator.getComponents().get(i).getBinaryOperator() instanceof ExampleAggregateFunction);
        assertArrayEquals(new String[]{"property4"},
                aggregator.getComponents().get(i).getSelection());
        i++;
        assertTrue(aggregator.getComponents().get(i).getBinaryOperator() instanceof ExampleAggregateFunction);
        assertArrayEquals(new String[]{"property5"},
                aggregator.getComponents().get(i).getSelection());
        i++;
        assertEquals(i, aggregator.getComponents().size());

        // Check the aggregator is locked.
        try {
            aggregator.getComponents().add(null);
            fail("Exception expected");
        } catch (final UnsupportedOperationException e) {
            assertNotNull(e);
        }
        // check the aggregator is cached
        assertSame(aggregator, elementDef.getIngestAggregator());
    }

    @Test
    public void shouldReturnQueryAggregator() {
        // Given
        final T elementDef = createBuilder()
                .property("property1", PROPERTY_STRING_TYPE)
                .property("property2", PROPERTY_STRING_TYPE)
                .property("property3", PROPERTY_STRING_TYPE)
                .property("visibility", PROPERTY_STRING_TYPE)
                .property("timestamp", PROPERTY_STRING_TYPE)
                .groupBy("property1", "property2")
                .build();

        setupSchema(elementDef);

        // When
        final ElementAggregator aggregator = elementDef.getQueryAggregator(Sets.newHashSet("property1"), null);

        // Then
        assertEquals(4, aggregator.getComponents().size());
        assertTrue(aggregator.getComponents().get(0).getBinaryOperator() instanceof ExampleAggregateFunction);
        assertArrayEquals(new String[]{"property2"},
                aggregator.getComponents().get(0).getSelection());
        assertTrue(aggregator.getComponents().get(1).getBinaryOperator() instanceof ExampleAggregateFunction);
        assertArrayEquals(new String[]{"property3"},
                aggregator.getComponents().get(1).getSelection());
        assertTrue(aggregator.getComponents().get(2).getBinaryOperator() instanceof ExampleAggregateFunction);
        assertArrayEquals(new String[]{"visibility"},
                aggregator.getComponents().get(2).getSelection());
        assertTrue(aggregator.getComponents().get(3).getBinaryOperator() instanceof ExampleAggregateFunction);
        assertArrayEquals(new String[]{"timestamp"},
                aggregator.getComponents().get(3).getSelection());
        // Check the aggregator is locked.
        try {
            aggregator.getComponents().add(null);
            fail("Exception expected");
        } catch (final UnsupportedOperationException e) {
            assertNotNull(e);
        }
        // check the aggregator is cached
        assertSame(aggregator, elementDef.getQueryAggregator(Sets.newHashSet("property1"), null));
        // check a different aggregator is returned for different groupBys
        assertNotSame(aggregator, elementDef.getQueryAggregator(Sets.newHashSet(), null));
    }

    @Test
    public void shouldReturnQueryAggregatorWithMultiPropertyAggregator() {
        // Given
        final T elementDef = createBuilder()
                .property("property1", PROPERTY_STRING_TYPE)
                .property("property2", PROPERTY_STRING_TYPE)
                .property("property3", PROPERTY_STRING_TYPE)
                .property("property4", PROPERTY_STRING_TYPE)
                .property("property5", PROPERTY_STRING_TYPE)
                .property("visibility", PROPERTY_STRING_TYPE)
                .property("timestamp", PROPERTY_STRING_TYPE)
                .groupBy("property1", "property2")
                .aggregator(new ElementAggregator.Builder()
                        .select("property1", "property2")
                        .execute(new ExampleTuple2BinaryOperator())
                        .select("property3", "property4")
                        .execute(new ExampleTuple2BinaryOperator())
                        .build())
                .build();

        setupSchema(elementDef);

        // When
        final ElementAggregator aggregator = elementDef.getQueryAggregator(Sets.newHashSet(), null);

        // Then
        assertEquals(5, aggregator.getComponents().size());
        assertTrue(aggregator.getComponents().get(0).getBinaryOperator() instanceof ExampleTuple2BinaryOperator);
        assertArrayEquals(new String[]{"property1", "property2"},
                aggregator.getComponents().get(0).getSelection());

        assertTrue(aggregator.getComponents().get(1).getBinaryOperator() instanceof ExampleTuple2BinaryOperator);
        assertArrayEquals(new String[]{"property3", "property4"},
                aggregator.getComponents().get(1).getSelection());

        assertTrue(aggregator.getComponents().get(2).getBinaryOperator() instanceof ExampleAggregateFunction);
        assertArrayEquals(new String[]{"property5"},
                aggregator.getComponents().get(2).getSelection());
        assertTrue(aggregator.getComponents().get(3).getBinaryOperator() instanceof ExampleAggregateFunction);
        assertArrayEquals(new String[]{"visibility"},
                aggregator.getComponents().get(3).getSelection());
        assertTrue(aggregator.getComponents().get(4).getBinaryOperator() instanceof ExampleAggregateFunction);
        assertArrayEquals(new String[]{"timestamp"},
                aggregator.getComponents().get(4).getSelection());

        // Check the aggregator is locked.
        try {
            aggregator.getComponents().add(null);
            fail("Exception expected");
        } catch (final UnsupportedOperationException e) {
            assertNotNull(e);
        }
        // check the aggregator is cached
        assertSame(aggregator, elementDef.getQueryAggregator(Sets.newHashSet(), null));
    }

    @Test
    public void shouldReturnQueryAggregatorWithViewAggregator() {
        // Given
        final T elementDef = createBuilder()
                .property("property1", PROPERTY_STRING_TYPE)
                .property("property2", PROPERTY_STRING_TYPE)
                .property("property3", PROPERTY_STRING_TYPE)
                .property("property4", PROPERTY_STRING_TYPE)
                .property("property5", PROPERTY_STRING_TYPE)
                .property("visibility", PROPERTY_STRING_TYPE)
                .property("timestamp", PROPERTY_STRING_TYPE)
                .groupBy("property1", "property2")
                .aggregator(new ElementAggregator.Builder()
                        .select("property3", "property4")
                        .execute(new ExampleTuple2BinaryOperator())
                        .build())
                .build();

        setupSchema(elementDef);

        final ElementAggregator viewAggregator = new ElementAggregator.Builder()
                .select("property1")
                .execute(new StringConcat())
                .build();

        // When
        final ElementAggregator aggregator = elementDef.getQueryAggregator(Sets.newHashSet(), viewAggregator);

        // Then
        int i = 0;
        assertTrue(aggregator.getComponents().get(i).getBinaryOperator() instanceof StringConcat);
        assertArrayEquals(new String[]{"property1"},
                aggregator.getComponents().get(i).getSelection());
        i++;
        assertTrue(aggregator.getComponents().get(i).getBinaryOperator() instanceof ExampleTuple2BinaryOperator);
        assertArrayEquals(new String[]{"property3", "property4"},
                aggregator.getComponents().get(i).getSelection());
        i++;
        assertTrue(aggregator.getComponents().get(i).getBinaryOperator() instanceof ExampleAggregateFunction);
        assertArrayEquals(new String[]{"property2"},
                aggregator.getComponents().get(i).getSelection());
        i++;
        assertTrue(aggregator.getComponents().get(i).getBinaryOperator() instanceof ExampleAggregateFunction);
        assertArrayEquals(new String[]{"property5"},
                aggregator.getComponents().get(i).getSelection());
        i++;
        assertTrue(aggregator.getComponents().get(i).getBinaryOperator() instanceof ExampleAggregateFunction);
        assertArrayEquals(new String[]{"visibility"},
                aggregator.getComponents().get(i).getSelection());
        i++;
        assertTrue(aggregator.getComponents().get(i).getBinaryOperator() instanceof ExampleAggregateFunction);
        assertArrayEquals(new String[]{"timestamp"},
                aggregator.getComponents().get(i).getSelection());
        i++;
        assertEquals(i, aggregator.getComponents().size());

        // Check the aggregator is locked.
        try {
            aggregator.getComponents().add(null);
            fail("Exception expected");
        } catch (final UnsupportedOperationException e) {
            assertNotNull(e);
        }
        // check the aggregator is not cached
        assertNotSame(aggregator, elementDef.getQueryAggregator(Sets.newHashSet(), viewAggregator));
    }

    @Test
    public void shouldReturnQueryAggregatorWithViewAggregatorAndMultipleAgg() {
        // Given
        final T elementDef = createBuilder()
                .property("property1", PROPERTY_STRING_TYPE)
                .property("property2", PROPERTY_STRING_TYPE)
                .property("property3", PROPERTY_STRING_TYPE)
                .property("property4", PROPERTY_STRING_TYPE)
                .property("property5", PROPERTY_STRING_TYPE)
                .property("visibility", PROPERTY_STRING_TYPE)
                .property("timestamp", PROPERTY_STRING_TYPE)
                .groupBy("property1", "property2")
                .aggregator(new ElementAggregator.Builder()
                        .select("property1", "property2")
                        .execute(new ExampleTuple2BinaryOperator())
                        .select("property3", "property4")
                        .execute(new ExampleTuple2BinaryOperator())
                        .build())
                .build();

        setupSchema(elementDef);

        final ElementAggregator viewAggregator = new ElementAggregator.Builder()
                .select("property1")
                .execute(new StringConcat())
                .build();

        // When
        final ElementAggregator aggregator = elementDef.getQueryAggregator(Sets.newHashSet(), viewAggregator);

        // Then
        int i = 0;
        assertTrue(aggregator.getComponents().get(i).getBinaryOperator() instanceof StringConcat);
        assertArrayEquals(new String[]{"property1"},
                aggregator.getComponents().get(i).getSelection());
        i++;
        assertTrue(aggregator.getComponents().get(i).getBinaryOperator() instanceof ExampleTuple2BinaryOperator);
        assertArrayEquals(new String[]{"property1", "property2"},
                aggregator.getComponents().get(i).getSelection());
        i++;
        assertTrue(aggregator.getComponents().get(i).getBinaryOperator() instanceof ExampleTuple2BinaryOperator);
        assertArrayEquals(new String[]{"property3", "property4"},
                aggregator.getComponents().get(i).getSelection());
        i++;
        assertTrue(aggregator.getComponents().get(i).getBinaryOperator() instanceof ExampleAggregateFunction);
        assertArrayEquals(new String[]{"property5"},
                aggregator.getComponents().get(i).getSelection());
        i++;
        assertTrue(aggregator.getComponents().get(i).getBinaryOperator() instanceof ExampleAggregateFunction);
        assertArrayEquals(new String[]{"visibility"},
                aggregator.getComponents().get(i).getSelection());
        i++;
        assertTrue(aggregator.getComponents().get(i).getBinaryOperator() instanceof ExampleAggregateFunction);
        assertArrayEquals(new String[]{"timestamp"},
                aggregator.getComponents().get(i).getSelection());
        i++;
        assertEquals(i, aggregator.getComponents().size());

        // Check the aggregator is locked.
        try {
            aggregator.getComponents().add(null);
            fail("Exception expected");
        } catch (final UnsupportedOperationException e) {
            assertNotNull(e);
        }
        // check the aggregator is not cached
        assertNotSame(aggregator, elementDef.getQueryAggregator(Sets.newHashSet(), viewAggregator));
    }

    @Test
    public void shouldReturnQueryAggregatorWithMultiPropertyAggregatorWithSingleGroupBy() {
        // Given
        final T elementDef = createBuilder()
                .property("property1", PROPERTY_STRING_TYPE)
                .property("property2", PROPERTY_STRING_TYPE)
                .property("property3", PROPERTY_STRING_TYPE)
                .property("property4", PROPERTY_STRING_TYPE)
                .property("property5", PROPERTY_STRING_TYPE)
                .property("visibility", PROPERTY_STRING_TYPE)
                .property("timestamp", PROPERTY_STRING_TYPE)
                .groupBy("property1")
                .aggregator(new ElementAggregator.Builder()
                        .select("property1", "property2")
                        .execute(new ExampleTuple2BinaryOperator())
                        .select("property3", "property4")
                        .execute(new ExampleTuple2BinaryOperator())
                        .build())
                .build();

        setupSchema(elementDef);

        // When
        final ElementAggregator aggregator = elementDef.getQueryAggregator(Sets.newHashSet(), null);

        // Then
        // As the groupBy property - property1 is aggregated alongside property2, this is still required in the aggregator function.
        assertEquals(5, aggregator.getComponents().size());
        assertTrue(aggregator.getComponents().get(0).getBinaryOperator() instanceof ExampleTuple2BinaryOperator);
        assertArrayEquals(new String[]{"property1", "property2"},
                aggregator.getComponents().get(0).getSelection());

        assertTrue(aggregator.getComponents().get(1).getBinaryOperator() instanceof ExampleTuple2BinaryOperator);
        assertArrayEquals(new String[]{"property3", "property4"},
                aggregator.getComponents().get(1).getSelection());

        assertTrue(aggregator.getComponents().get(2).getBinaryOperator() instanceof ExampleAggregateFunction);
        assertArrayEquals(new String[]{"property5"},
                aggregator.getComponents().get(2).getSelection());
        assertTrue(aggregator.getComponents().get(3).getBinaryOperator() instanceof ExampleAggregateFunction);
        assertArrayEquals(new String[]{"visibility"},
                aggregator.getComponents().get(3).getSelection());
        assertTrue(aggregator.getComponents().get(4).getBinaryOperator() instanceof ExampleAggregateFunction);
        assertArrayEquals(new String[]{"timestamp"},
                aggregator.getComponents().get(4).getSelection());

        // Check the aggregator is locked.
        try {
            aggregator.getComponents().add(null);
            fail("Exception expected");
        } catch (final UnsupportedOperationException e) {
            assertNotNull(e);
        }
        // check the aggregator is cached
        assertSame(aggregator, elementDef.getQueryAggregator(Sets.newHashSet(), null));
    }

    @Test
    public void shouldMergeDifferentSchemaElementDefinitions() {
        // Given
        // When
        final T elementDef1 = createBuilder()
                .property(TestPropertyNames.PROP_1, "property.integer")
                .validator(new ElementFilter.Builder()
                        .select(TestPropertyNames.PROP_1)
                        .execute(new ExampleFilterFunction())
                        .build())
                .build();

        final T elementDef2 = createBuilder()
                .property(TestPropertyNames.PROP_2, "property.object")
                .validator(new ElementFilter.Builder()
                        .select(TestPropertyNames.PROP_2)
                        .execute(new ExampleFilterFunction())
                        .build())
                .groupBy(TestPropertyNames.PROP_2)
                .build();

        // When
        final T mergedDef = createEmptyBuilder()
                .merge(elementDef1)
                .merge(elementDef2)
                .build();

        // Then
        assertEquals(2, mergedDef.getProperties().size());
        assertNotNull(mergedDef.getPropertyTypeDef(TestPropertyNames.PROP_1));
        assertNotNull(mergedDef.getPropertyTypeDef(TestPropertyNames.PROP_2));
        assertEquals(Sets.newLinkedHashSet(Collections.singletonList(TestPropertyNames.PROP_2)),
                mergedDef.getGroupBy());
    }

    @Test
    public void shouldThrowExceptionWhenMergeSchemaElementDefinitionWithConflictingProperty() {
        // Given
        // When
        final T elementDef1 = createBuilder()
                .property(TestPropertyNames.PROP_1, "string")
                .build();

        final T elementDef2 = createBuilder()
                .property(TestPropertyNames.PROP_1, "int")
                .build();

        // When / Then
        try {
            createEmptyBuilder()
                    .merge(elementDef1)
                    .merge(elementDef2)
                    .build();
            fail("Exception expected");
        } catch (final SchemaException e) {
            assertTrue(e.getMessage().contains("property"));
        }
    }

    protected void setupSchema(final T elementDef) {
        final Schema.Builder schemaBuilder = new Schema.Builder()
                .type("id.integer", Integer.class)
                .type("id.date", Date.class)
                .type("directed.true", Boolean.class)
                .type("property.integer", Integer.class)
                .type("property.object", Object.class)
                .type(PROPERTY_STRING_TYPE, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .aggregateFunction(new ExampleAggregateFunction())
                        .validateFunctions(new Exists())
                        .build())
                .visibilityProperty("visibility")
                .timestampProperty("timestamp");
        if (elementDef instanceof SchemaEdgeDefinition) {
            schemaBuilder.edge(TestGroups.EDGE, (SchemaEdgeDefinition) elementDef);
        } else {
            schemaBuilder.entity(TestGroups.ENTITY, (SchemaEntityDefinition) elementDef);
        }
        schemaBuilder.build();
    }
}