/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.codelibs.elasticsearch.taste.common;

import java.io.Serializable;

import com.google.common.primitives.Longs;

/** A simple (ordered) pair of longs. */
public final class LongPair implements Comparable<LongPair>, Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private final long first;

    private final long second;

    public LongPair(final long first, final long second) {
        this.first = first;
        this.second = second;
    }

    public long getFirst() {
        return first;
    }

    public long getSecond() {
        return second;
    }

    public LongPair swap() {
        return new LongPair(second, first);
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof LongPair)) {
            return false;
        }
        final LongPair otherPair = (LongPair) obj;
        return first == otherPair.getFirst() && second == otherPair.getSecond();
    }

    @Override
    public int hashCode() {
        final int firstHash = Longs.hashCode(first);
        // Flip top and bottom 16 bits; this makes the hash function probably different
        // for (a,b) versus (b,a)
        return (firstHash >>> 16 | firstHash << 16) ^ Longs.hashCode(second);
    }

    @Override
    public String toString() {
        return '(' + String.valueOf(first) + ',' + second + ')';
    }

    @Override
    public int compareTo(final LongPair o) {
        if (first < o.getFirst()) {
            return -1;
        } else if (first > o.getFirst()) {
            return 1;
        } else {
            return second < o.getSecond() ? -1 : second > o.getSecond() ? 1 : 0;
        }
    }

}
