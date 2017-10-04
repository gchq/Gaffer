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

package uk.gov.gchq.gaffer.flink.operation.handler;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.flink.operation.FlinkTest;
import uk.gov.gchq.gaffer.operation.Validatable;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class GafferAdderTest {
    @Test
    public void shouldAddElementsToStore() throws Exception {
        // Given
        final Validatable op = mock(Validatable.class);
        final Store store = mock(Store.class);
        given(store.getProperties()).willReturn(new StoreProperties());
        given(store.getSchema()).willReturn(new Schema());
        given(op.isValidate()).willReturn(true);
        given(op.isSkipInvalidElements()).willReturn(false);

        final GafferAdder adder = new GafferAdder(op, store);

        // When
        adder.add(FlinkTest.EXPECTED_ELEMENTS);

        // Then
        final ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(store).runAsync(runnableCaptor.capture());
        runnableCaptor.getValue().run();
        verify(store).execute(Mockito.eq(new AddElements.Builder()
                .input(new GafferQueue<>(new ConcurrentLinkedQueue<>(FlinkTest.EXPECTED_ELEMENTS)))
                .validate(true)
                .skipInvalidElements(false)
                .build()), Mockito.any());
    }

    @Test
    public void shouldRestartAddElementsIfPauseInIngest() throws Exception {
        // Given
        final Validatable op = mock(Validatable.class);
        final Store store = mock(Store.class);
        given(store.getProperties()).willReturn(new StoreProperties());
        given(store.getSchema()).willReturn(new Schema());
        given(op.isValidate()).willReturn(true);
        given(op.isSkipInvalidElements()).willReturn(false);

        final GafferAdder adder = new GafferAdder(op, store);

        // When
        adder.add(FlinkTest.EXPECTED_ELEMENTS);

        // Then
        final ArgumentCaptor<Runnable> runnableCaptor1 = ArgumentCaptor.forClass(Runnable.class);
        verify(store).runAsync(runnableCaptor1.capture());
        runnableCaptor1.getValue().run();
        final ConcurrentLinkedQueue<Element> expectedQueue = new ConcurrentLinkedQueue<>(FlinkTest.EXPECTED_ELEMENTS);
        verify(store).execute(Mockito.eq(new AddElements.Builder()
                .input(new GafferQueue<>(expectedQueue))
                .validate(true)
                .skipInvalidElements(false)
                .build()), Mockito.any());
        Mockito.reset(store);

        // When
        adder.add(FlinkTest.EXPECTED_ELEMENTS_2);

        // Then
        final ArgumentCaptor<Runnable> runnableCaptor2 = ArgumentCaptor.forClass(Runnable.class);
        verify(store).runAsync(runnableCaptor2.capture());
        runnableCaptor2.getValue().run();
        // As the queue has not been consumed the original elements will still be on the queue.
        expectedQueue.addAll(FlinkTest.EXPECTED_ELEMENTS_2);
        verify(store).execute(Mockito.eq(new AddElements.Builder()
                .input(new GafferQueue<>(expectedQueue))
                .validate(true)
                .skipInvalidElements(false)
                .build()), Mockito.any());
    }

    @Test
    public void shouldAddElementsIfInvokeCalledMultipleTimes() throws Exception {
        // Given
        final int duplicates = 4;
        final Validatable op = mock(Validatable.class);
        final Store store = mock(Store.class);
        given(store.getProperties()).willReturn(new StoreProperties());
        given(store.getSchema()).willReturn(new Schema());
        given(op.isValidate()).willReturn(true);
        given(op.isSkipInvalidElements()).willReturn(false);

        final GafferAdder adder = new GafferAdder(op, store);

        // When
        for (int i = 0; i < duplicates; i++) {
            adder.add(FlinkTest.EXPECTED_ELEMENTS);
        }

        // Then
        final ArgumentCaptor<Runnable> runnableCaptor1 = ArgumentCaptor.forClass(Runnable.class);
        verify(store).runAsync(runnableCaptor1.capture());
        assertEquals(1, runnableCaptor1.getAllValues().size());
        runnableCaptor1.getValue().run();
        final ConcurrentLinkedQueue<Element> expectedQueue = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < duplicates; i++) {
            expectedQueue.addAll(FlinkTest.EXPECTED_ELEMENTS);
        }
        verify(store).execute(Mockito.eq(new AddElements.Builder()
                .input(new GafferQueue<>(new ConcurrentLinkedQueue<>(expectedQueue)))
                .validate(true)
                .skipInvalidElements(false)
                .build()), Mockito.any());
    }
}
