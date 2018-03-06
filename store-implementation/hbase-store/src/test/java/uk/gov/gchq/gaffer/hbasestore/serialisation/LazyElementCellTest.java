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
package uk.gov.gchq.gaffer.hbasestore.serialisation;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.junit.Test;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.exception.SerialisationException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class LazyElementCellTest {
    @Test
    public void shouldConstructLazyElementCell() throws SerialisationException {
        // Given
        final Cell cell = mock(Cell.class);
        final ElementSerialisation serialisation = mock(ElementSerialisation.class);
        final Element element = mock(Element.class);

        given(serialisation.getElement(cell, false)).willReturn(element);

        // When
        final LazyElementCell lazyElementCell = new LazyElementCell(cell, serialisation, false);

        // Then
        assertSame(cell, lazyElementCell.getCell());
        assertFalse(lazyElementCell.isElementLoaded());
        assertSame(element, lazyElementCell.getElement());
        assertTrue(lazyElementCell.isElementLoaded());
        assertSame(serialisation, lazyElementCell.getSerialisation());
    }

    @Test
    public void shouldNotBeAbleToDeserialiseCellIfCellIsMarkedForDeletion() throws SerialisationException {
        // Given
        final Cell cell = mock(Cell.class);
        final ElementSerialisation serialisation = mock(ElementSerialisation.class);
        final LazyElementCell lazyElementCell = new LazyElementCell(cell, serialisation, false);

        given(cell.getTypeByte()).willReturn(KeyValue.Type.Delete.getCode());

        // When / Then
        try {
            lazyElementCell.getElement();
            fail("Exception expected");
        } catch (final IllegalStateException e) {
            assertNotNull(e.getMessage());
        }

        assertTrue(lazyElementCell.isDeleted());
    }

    @Test
    public void shoulCacheLoadedElement() throws SerialisationException {
        // Given
        final Cell cell = mock(Cell.class);
        final ElementSerialisation serialisation = mock(ElementSerialisation.class);
        final Element element = mock(Element.class);

        given(serialisation.getElement(cell, false)).willReturn(element);

        // When
        final LazyElementCell lazyElementCell = new LazyElementCell(cell, serialisation, false);

        // Then
        assertFalse(lazyElementCell.isElementLoaded());
        assertSame(element, lazyElementCell.getElement());
        assertTrue(lazyElementCell.isElementLoaded());
        assertSame(element, lazyElementCell.getElement());
        verify(serialisation, times(1)).getElement(cell, false);
        assertSame(serialisation, lazyElementCell.getSerialisation());
    }


    @Test
    public void shouldCacheGroup() throws SerialisationException {
        // Given
        final Cell cell = mock(Cell.class);
        final ElementSerialisation serialisation = mock(ElementSerialisation.class);
        final String group = "a group";

        given(serialisation.getGroup(cell)).willReturn(group);

        // When
        final LazyElementCell lazyElementCell = new LazyElementCell(cell, serialisation, false);

        // Then
        assertEquals(group, lazyElementCell.getGroup());
        assertEquals(group, lazyElementCell.getGroup());
        assertFalse(lazyElementCell.isElementLoaded());
        verify(serialisation, times(1)).getGroup(cell);
    }
}
