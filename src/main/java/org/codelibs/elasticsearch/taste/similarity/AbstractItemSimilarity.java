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

package org.codelibs.elasticsearch.taste.similarity;

import java.util.Collection;

import org.codelibs.elasticsearch.taste.common.FastIDSet;
import org.codelibs.elasticsearch.taste.common.LongPrimitiveIterator;
import org.codelibs.elasticsearch.taste.common.RefreshHelper;
import org.codelibs.elasticsearch.taste.common.Refreshable;
import org.codelibs.elasticsearch.taste.model.DataModel;

import com.google.common.base.Preconditions;

public abstract class AbstractItemSimilarity implements ItemSimilarity {

    private final DataModel dataModel;

    private final RefreshHelper refreshHelper;

    protected AbstractItemSimilarity(final DataModel dataModel) {
        Preconditions.checkArgument(dataModel != null, "dataModel is null");
        this.dataModel = dataModel;
        refreshHelper = new RefreshHelper(null);
        refreshHelper.addDependency(this.dataModel);
    }

    protected DataModel getDataModel() {
        return dataModel;
    }

    @Override
    public long[] allSimilarItemIDs(final long itemID) {
        final FastIDSet allSimilarItemIDs = new FastIDSet();
        final LongPrimitiveIterator allItemIDs = dataModel.getItemIDs();
        while (allItemIDs.hasNext()) {
            final long possiblySimilarItemID = allItemIDs.nextLong();
            if (!Double.isNaN(itemSimilarity(itemID, possiblySimilarItemID))) {
                allSimilarItemIDs.add(possiblySimilarItemID);
            }
        }
        return allSimilarItemIDs.toArray();
    }

    @Override
    public void refresh(final Collection<Refreshable> alreadyRefreshed) {
        refreshHelper.refresh(alreadyRefreshed);
    }
}
