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
package gaffer.gafferpop;

public class EdgeId {
    private final Object source;
    private final Object dest;

    public EdgeId(final Object source, final Object dest) {
        this.source = source;
        this.dest = dest;
    }

    public Object getSource() {
        return source;
    }

    public Object getDest() {
        return dest;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final EdgeId edgeId = (EdgeId) o;

        if (!source.equals(edgeId.source)) {
            return false;
        }

        return dest.equals(edgeId.dest);

    }

    @Override
    public int hashCode() {
        int result = source.hashCode();
        result = 31 * result + dest.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return source + "->" + dest;
    }
}
