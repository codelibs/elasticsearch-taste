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

import org.codelibs.elasticsearch.taste.common.FastIDSet;
import org.codelibs.elasticsearch.taste.model.DataModel;
import org.codelibs.elasticsearch.taste.similarity.ItemSimilarity;

import com.google.common.base.Preconditions;

/**
 * returns the result of {@link ItemSimilarity#allSimilarItemIDs(long)} as candidate items
 */
public class AllSimilarItemsCandidateItemsStrategy extends
        AbstractCandidateItemsStrategy {

    private final ItemSimilarity similarity;

    public AllSimilarItemsCandidateItemsStrategy(final ItemSimilarity similarity) {
        Preconditions.checkArgument(similarity != null, "similarity is null");
        this.similarity = similarity;
    }

    @Override
    protected FastIDSet doGetCandidateItems(final long[] preferredItemIDs,
            final DataModel dataModel) {
        final FastIDSet candidateItemIDs = new FastIDSet();
        for (final long itemID : preferredItemIDs) {
            candidateItemIDs.addAll(similarity.allSimilarItemIDs(itemID));
        }
        candidateItemIDs.removeAll(preferredItemIDs);
        return candidateItemIDs;
    }
}
