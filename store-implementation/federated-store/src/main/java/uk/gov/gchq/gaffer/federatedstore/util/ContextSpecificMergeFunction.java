package uk.gov.gchq.gaffer.federatedstore.util;

import uk.gov.gchq.gaffer.core.exception.GafferCheckedException;

import java.util.HashMap;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * a function which is configurable based on the operation.
 *
 * @param <T> – the type of the first argument to the function
 * @param <U> – the type of the second argument to the function
 * @param <R> – the type of the result of the function
 * @see BiFunction
 */
public interface ContextSpecificMergeFunction<T, U, R> extends BiFunction<T, U, R> {
    ContextSpecificMergeFunction<T, U, R> createFunctionWithContext(final HashMap<String, Object> context) throws GafferCheckedException; //TODO FS do I want a checkedException?

    default boolean isRequired(final String name) {
        return getRequiredContextValues().contains(name);
    }

    Set<String> getRequiredContextValues();
}
