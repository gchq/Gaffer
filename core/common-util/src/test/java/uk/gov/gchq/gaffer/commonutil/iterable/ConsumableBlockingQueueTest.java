/*
 * Copyright 2017-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.commonutil.iterable;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class ConsumableBlockingQueueTest {

    @Test
    void shouldConsumeResultsWhenIterating() throws InterruptedException {
        // Given
        final ConsumableBlockingQueue<Integer> queue = new ConsumableBlockingQueue<>(5);

        for (int i = 0; i < 4; i++) {
            queue.put(i);
        }

        // When
        final List<Integer> items = queue.stream().collect(Collectors.toList());

        // Then
        assertThat(items).containsExactly(0, 1, 2, 3);
        assertThat(queue).isEmpty();

        // Iterate a second time and the queue should not have any values
        final List<Integer> items2 = queue.stream().collect(Collectors.toList());
        assertThat(items2).isEmpty();
    }

    @Test
    void shouldBlockOnAdditionWhenQueueIsFull() throws InterruptedException {
        // Given
        final ConsumableBlockingQueue<Integer> queue = new ConsumableBlockingQueue<>(5);

        final boolean[] finishedAdding = new boolean[] {false};
        new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                try {
                    queue.put(i);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            finishedAdding[0] = true;
        }).start();

        // Wait for some items to be added, but there isn't room for all of them
        Thread.sleep(1000L);
        assertThat(finishedAdding[0]).isFalse();

        // Consume some results
        final Iterator<Integer> consumer = queue.iterator();
        final List<Integer> items = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            assertThat(consumer).hasNext();
            items.add(consumer.next());
        }

        // Now the queue has space some items should be added, but there still isn't room for all of them
        Thread.sleep(1000L);
        assertThat(finishedAdding[0]).isFalse();

        // Consume some more results
        for (int i = 0; i < 4; i++) {
            assertThat(consumer).hasNext();
            items.add(consumer.next());
        }

        // Now the queue has space some items should be added and this time there is room for the rest of them
        Thread.sleep(1000L);
        assertThat(finishedAdding[0]).isTrue();

        // Consume some rest of the results
        while (consumer.hasNext()) {
            items.add(consumer.next());
        }

        // Then
        assertThat(items).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @Test
    void shouldNotBlockWhenConsumingWhenQueueIsEmpty() {
        final ConsumableBlockingQueue<Integer> queue = new ConsumableBlockingQueue<>(5);

        final Iterator<Integer> iterator = queue.iterator();

        assertThat(iterator).isExhausted();
    }

    @Test
    void shouldThrowExceptionWhenQueueIsEmpty() {
        final ConsumableBlockingQueue<Integer> queue = new ConsumableBlockingQueue<>(5);

        final Iterator<Integer> iterator = queue.iterator();

        assertThatExceptionOfType(NoSuchElementException.class)
            .isThrownBy(() -> iterator.next())
            .withMessage("No more items");
    }

    @Test
    void shouldReturnToString() throws InterruptedException {
        final ConsumableBlockingQueue<Integer> queue = new ConsumableBlockingQueue<>(5);

        for (int i = 0; i < 4; i++) {
            queue.put(i);
        }

        assertThat(queue).hasToString("ConsumableBlockingQueue[items={0,1,2,3}]");
    }
}
