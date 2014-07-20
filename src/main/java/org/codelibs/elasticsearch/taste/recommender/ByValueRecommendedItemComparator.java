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

package org.codelibs.elasticsearch.taste.recommender;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Defines a natural ordering from most-preferred item (highest value) to least-preferred.
 */
public final class ByValueRecommendedItemComparator implements
        Comparator<RecommendedItem>, Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private static final Comparator<RecommendedItem> INSTANCE = new ByValueRecommendedItemComparator();

    public static Comparator<RecommendedItem> getInstance() {
        return INSTANCE;
    }

    @Override
    public int compare(final RecommendedItem o1, final RecommendedItem o2) {
        final float value1 = o1.getValue();
        final float value2 = o2.getValue();
        return value1 > value2 ? -1 : value1 < value2 ? 1 : 0;
    }

}
