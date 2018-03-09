/*
 * Copyright 2017-2018 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.CloseableUtil;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A {@code BatchedIterable} is an iterable of batches.
 * To use, extend this class and implement the createBatch method. When you have
 * no more batches of items left, return null.
 * <p>
 * As a client iterators round this iterable it will first create a batch, then iterate
 * around the batch. When there are no more items in the batch createBatch will be
 * called again. This should only be used in a single thread and only 1 iterator
 * can be used at a time.
 *
 * @param <T> the type of items in the iterable.
 */
public abstract class BatchedIterable<T> implements CloseableIterable<T> {
    private Iterable<T> batch;
    private Iterator<T> batchIterator;

    @Override
    public CloseableIterator<T> iterator() {
        // By design, only 1 iterator can be open at a time
        closeBatch();

        return new BatchedIterator();
    }

    @Override
    public void close() {
        CloseableUtil.close(batchIterator);
        CloseableUtil.close(batch);
    }

    /**
     * @return the next batch of items. If null, then it is assumed there are no more batches.
     */
    protected abstract Iterable<T> createBatch();

    private void closeBatch() {
        if (null != batchIterator) {
            CloseableUtil.close(batchIterator);
            batchIterator = null;
        }

        if (null != batch) {
            CloseableUtil.close(batch);
            batch = null;
        }
    }

    private final class BatchedIterator implements CloseableIterator<T> {
        @Override
        public boolean hasNext() {
            if (null != batchIterator && batchIterator.hasNext()) {
                return true;
            }

            boolean hasNext = false;
            while (!hasNext) {
                closeBatch();
                batch = createBatch();
                if (null == batch) {
                    batchIterator = Collections.emptyIterator();
                    break;
                } else {
                    batchIterator = batch.iterator();
                    hasNext = batchIterator.hasNext();
                }
            }

            return hasNext;
        }

        @Override
        public T next() {
            if (null == batchIterator) {
                throw new NoSuchElementException("Reached the end of the batch iterator");
            }
            return batchIterator.next();
        }

        @Override
        public void close() {
            closeBatch();
        }
    }
}
