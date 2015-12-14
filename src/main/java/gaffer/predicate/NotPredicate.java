/**
 * Copyright 2015 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gaffer.predicate;

import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Creates a {@link Predicate} by taking the opposite of the provided {@link Predicate}.
 *
 * @param <T>
 */
public class NotPredicate<T> implements Predicate<T> {

	private static final long serialVersionUID = 2670350197106124001L;
	private Predicate<T> predicate;

	public NotPredicate() { }

	public NotPredicate(Predicate<T> predicate) {
		this.predicate = predicate;
	}

	@Override
	public boolean accept(T t) throws IOException {
		return !predicate.accept(t);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, predicate.getClass().getName());
		predicate.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		try {
			String className = Text.readString(in);
			predicate = (Predicate<T>) Class.forName(className).newInstance();
			predicate.readFields(in);
		} catch (InstantiationException e) {
			throw new IOException("Exception deserialising predicate in " + this.getClass().getName() + ": " + e);
		} catch (IllegalAccessException e) {
			throw new IOException("Exception deserialising predicate in " + this.getClass().getName() + ": " + e);
		} catch (ClassNotFoundException e) {
			throw new IOException("Exception deserialising predicate in " + this.getClass().getName() + ": " + e);
		} catch (ClassCastException e) {
			throw new IOException("Exception deserialising predicate in " + this.getClass().getName() + ": " + e);
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((predicate == null) ? 0 : predicate.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		NotPredicate other = (NotPredicate) obj;
		if (predicate == null) {
			if (other.predicate != null)
				return false;
		} else if (!predicate.equals(other.predicate)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return "NotPredicate [predicate=" + predicate + "]";
	}

}
