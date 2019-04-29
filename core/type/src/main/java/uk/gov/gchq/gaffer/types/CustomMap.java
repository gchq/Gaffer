/*
 * Copyright 2019 Crown Copyright
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
package uk.gov.gchq.gaffer.types;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

@JsonPropertyOrder(value = {"class", "keySerialiser", "valueSerialiser", "jsonStorage"})
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "class")
public class CustomMap<K, V> {
    private final ToBytesSerialiser<? super K> keySerialiser;
    private final ToBytesSerialiser<? super V> valueSerialiser;
    private final HashMap<K, V> delegateMap;

    public CustomMap(final ToBytesSerialiser<? super K> keySerialiser, final ToBytesSerialiser<? super V> valueSerialiser) {
        this(keySerialiser, valueSerialiser, (Map<K, V>) null);
    }

    @JsonCreator
    public CustomMap(@JsonProperty("keySerialiser") final ToBytesSerialiser<? super K> keySerialiser, @JsonProperty("valueSerialiser") final ToBytesSerialiser<? super V> valueSerialiser, @JsonProperty("jsonStorage") final HashSet<Pair<K, V>> interimPairs) {
        this(keySerialiser, valueSerialiser, interimToMap(interimPairs));
    }

    private static <K, V> Map<K, V> interimToMap(final HashSet<Pair<K, V>> interimPairs) {
        final HashMap<K, V> rtn = new HashMap<>();
        for (final Pair<K, V> pair : interimPairs) {
            final K first = pair.getFirst();
            final V old = rtn.put(first, pair.getSecond());
            if (null != old) {
                throw new IllegalArgumentException("Error in constructor, the interim storage used for JSONSerialiser did not contain unique keys. key:" + first + " value:" + old);
            }
        }
        return rtn;
    }

    public CustomMap(final ToBytesSerialiser<? super K> keySerialiser, final ToBytesSerialiser<? super V> valueSerialiser, final Map<K, V> storageMap) {
        requireNonNull(keySerialiser, "keySerialiser constructor parameter can't be null");
        requireNonNull(valueSerialiser, "valueSerialiser constructor parameter can't be null");
        this.keySerialiser = keySerialiser;
        this.valueSerialiser = valueSerialiser;
        this.delegateMap = new HashMap<>();

        if (nonNull(storageMap) && !storageMap.isEmpty()) {
            constructorValidation(storageMap);
            this.delegateMap.putAll(storageMap);
        }
    }

    protected void constructorValidation(final Map<K, V> storageMap) {
        final K anyK = storageMap.keySet().iterator().next();
        final Class<?> keyClass = null == anyK ? null : anyK.getClass();
        final V anyV = storageMap.values().iterator().next();
        final Class<?> valueClass = null == anyV ? null : anyV.getClass();
        final boolean canHandleKey = keySerialiser.canHandle(keyClass);
        final boolean canHandleValue = valueSerialiser.canHandle(valueClass);
        if (!canHandleKey || !canHandleValue) {
            final StringBuilder sb = new StringBuilder("Error in constructor, mismatched");
            if (!canHandleKey) {
                sb.append(" key with keySerialiser");
                sb.append(" Key:" + ((null == keyClass) ? null : keyClass.getSimpleName()) + ", Serialiser:" + keySerialiser.getClass().getSimpleName());
            }
            if (!canHandleKey && !canHandleValue) {
                sb.append(" and");
            }
            if (!canHandleValue) {
                sb.append(" value with valueSerialiser");
                sb.append(" Value:" + ((null == valueClass) ? null : valueClass.getSimpleName()) + ", Serialiser:" + valueSerialiser.getClass().getSimpleName());
            }
            throw new IllegalArgumentException(sb.toString());
        }
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "class")
    public ToBytesSerialiser<? super K> getKeySerialiser() {
        return this.keySerialiser;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "class")
    public ToBytesSerialiser<? super V> getValueSerialiser() {
        return this.valueSerialiser;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.WRAPPER_OBJECT, property = "class")
    public HashSet<Pair<K, V>> getJsonStorage() {
        final HashSet<Pair<K, V>> pairs = new HashSet<>();
        for (final Map.Entry<K, V> entry : this.delegateMap.entrySet()) {
            final Pair<K, V> p = new Pair<>(entry.getKey(), entry.getValue());
            pairs.add(p);
        }
        return pairs;
    }

    @JsonIgnore
    public HashMap<K, V> getMap() {
        return delegateMap;
    }

    public int size() {
        return delegateMap.size();
    }

    @JsonIgnore
    public boolean isEmpty() {
        return delegateMap.isEmpty();
    }

    public V get(final Object key) {
        return delegateMap.get(key);
    }

    public boolean containsKey(final Object key) {
        return delegateMap.containsKey(key);
    }

    public V put(final K key, final V value) {
        return delegateMap.put(key, value);
    }

    public void putAll(final Map<? extends K, ? extends V> m) {
        delegateMap.putAll(m);
    }

    public V remove(final Object key) {
        return delegateMap.remove(key);
    }

    public void clear() {
        delegateMap.clear();
    }

    public boolean containsValue(final Object value) {
        return delegateMap.containsValue(value);
    }

    public Set<K> keySet() {
        return delegateMap.keySet();
    }

    public Collection<V> values() {
        return delegateMap.values();
    }

    public Set<Map.Entry<K, V>> entrySet() {
        return delegateMap.entrySet();
    }

    public V getOrDefault(final Object key, final V defaultValue) {
        return delegateMap.getOrDefault(key, defaultValue);
    }

    public V putIfAbsent(final K key, final V value) {
        return delegateMap.putIfAbsent(key, value);
    }

    public boolean remove(final Object key, final Object value) {
        return delegateMap.remove(key, value);
    }

    public boolean replace(final K key, final V oldValue, final V newValue) {
        return delegateMap.replace(key, oldValue, newValue);
    }

    public V replace(final K key, final V value) {
        return delegateMap.replace(key, value);
    }

    public V computeIfAbsent(final K key, final Function<? super K, ? extends V> mappingFunction) {
        return delegateMap.computeIfAbsent(key, mappingFunction);
    }

    public V computeIfPresent(final K key, final BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        return delegateMap.computeIfPresent(key, remappingFunction);
    }

    public V compute(final K key, final BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        return delegateMap.compute(key, remappingFunction);
    }

    public V merge(final K key, final V value, final BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        return delegateMap.merge(key, value, remappingFunction);
    }

    public void forEach(final BiConsumer<? super K, ? super V> action) {
        delegateMap.forEach(action);
    }

    public void replaceAll(final BiFunction<? super K, ? super V, ? extends V> function) {
        delegateMap.replaceAll(function);
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final CustomMap serialiser = (CustomMap) obj;

        return new EqualsBuilder()
                .append(keySerialiser, serialiser.keySerialiser)
                .append(valueSerialiser, serialiser.valueSerialiser)
                .append(delegateMap, serialiser.delegateMap)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(89, 41)
                .append(keySerialiser)
                .append(valueSerialiser)
                .append(delegateMap)
                .hashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("keySerialiser", keySerialiser)
                .append("valueSerialiser", valueSerialiser)
                .append("delegateMap", delegateMap)
                .toString();
    }
}
