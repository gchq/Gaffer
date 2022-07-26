package uk.gov.gchq.gaffer.federatedstore.util;

import java.util.HashMap;
import java.util.function.BiFunction;

/**
 * a function which is configuratble based on the operation.
 *
 *
 * @param <T>
 * @param <U>
 * @param <R>
 */
public interface ContextSpecificMergeFunction<T, U, R> extends BiFunction<T, U, R> {
    ContextSpecificMergeFunction<T, U, R> createFunctionWithContext(final HashMap<String, Object> context);
}
