/*
 * Copyright 2017-2021 Crown Copyright
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConsumableBlockingQueueTest {

    @Test
    public void shouldConsumeResultsWhenIterating() {
        // Given
        final ConsumableBlockingQueue<Integer> queue = new ConsumableBlockingQueue<>(5);

        IntStream.range(0, 4)
                .forEach(i -> {
                    try {
                        queue.put(i);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });

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
    public void shouldBlockOnAdditionWhenQueueIsFull() throws InterruptedException {
        // Given
        final ConsumableBlockingQueue<Integer> queue = new ConsumableBlockingQueue<>(5);

        final boolean[] finishedAdding = new boolean[] {false};
        new Thread(() -> {
            IntStream.range(0, 10)
                    .forEach(i -> {
                        try {
                            queue.put(i);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    });
            finishedAdding[0] = true;
        }).start();

        // Wait for some items to be added, but there isn't room for all of them
        Thread.sleep(1000L);
        assertFalse(finishedAdding[0]);

        // Consume some results
        final Iterator<Integer> consumer = queue.iterator();
        final List<Integer> items = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            assertThat(consumer).hasNext();
            items.add(consumer.next());
        }

        // Now the queue has space some items should be added, but there still isn't room for all of them
        Thread.sleep(1000L);
        assertFalse(finishedAdding[0]);

        // Consume some more results
        for (int i = 0; i < 4; i++) {
            assertThat(consumer).hasNext();
            items.add(consumer.next());
        }

        // Now the queue has space some items should be added and this time there is room for the rest of them
        Thread.sleep(1000L);
        assertTrue(finishedAdding[0]);

        // Consume some rest of the results
        while (consumer.hasNext()) {
            items.add(consumer.next());
        }

        // Then
        assertThat(items).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @Test
    public void shouldNotBlockWhenConsumingWhenQueueIsEmpty() {
        final ConsumableBlockingQueue<Integer> queue = new ConsumableBlockingQueue<>(5);

        final Iterator<Integer> iterator = queue.iterator();

        assertThat(iterator).isExhausted();
    }
}
