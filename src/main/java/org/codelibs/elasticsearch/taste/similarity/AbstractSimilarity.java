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

import org.codelibs.elasticsearch.taste.common.RefreshHelper;
import org.codelibs.elasticsearch.taste.common.Refreshable;
import org.codelibs.elasticsearch.taste.common.Weighting;
import org.codelibs.elasticsearch.taste.model.DataModel;
import org.codelibs.elasticsearch.taste.model.PreferenceArray;

import com.google.common.base.Preconditions;

/** Abstract superclass encapsulating functionality that is common to most implementations in this package. */
abstract class AbstractSimilarity extends AbstractItemSimilarity implements
        UserSimilarity {

    private PreferenceInferrer inferrer;

    private final boolean weighted;

    private final boolean centerData;

    private int cachedNumItems;

    private int cachedNumUsers;

    private final RefreshHelper refreshHelper;

    /**
     * <p>
     * Creates a possibly weighted {@link AbstractSimilarity}.
     * </p>
     */
    AbstractSimilarity(final DataModel dataModel, final Weighting weighting,
            final boolean centerData) {
        super(dataModel);
        weighted = weighting == Weighting.WEIGHTED;
        this.centerData = centerData;
        cachedNumItems = dataModel.getNumItems();
        cachedNumUsers = dataModel.getNumUsers();
        refreshHelper = new RefreshHelper(() -> {
            cachedNumItems = dataModel.getNumItems();
            cachedNumUsers = dataModel.getNumUsers();
            return null;
        });
    }

    final PreferenceInferrer getPreferenceInferrer() {
        return inferrer;
    }

    @Override
    public final void setPreferenceInferrer(final PreferenceInferrer inferrer) {
        Preconditions.checkArgument(inferrer != null, "inferrer is null");
        refreshHelper.addDependency(inferrer);
        refreshHelper.removeDependency(this.inferrer);
        this.inferrer = inferrer;
    }

    final boolean isWeighted() {
        return weighted;
    }

    /**
     * <p>
     * Several subclasses in this package implement this method to actually compute the similarity from figures
     * computed over users or items. Note that the computations in this class "center" the data, such that X and
     * Y's mean are 0.
     * </p>
     *
     * <p>
     * Note that the sum of all X and Y values must then be 0. This value isn't passed down into the standard
     * similarity computations as a result.
     * </p>
     *
     * @param n
     *          total number of users or items
     * @param sumXY
     *          sum of product of user/item preference values, over all items/users preferred by both
     *          users/items
     * @param sumX2
     *          sum of the square of user/item preference values, over the first item/user
     * @param sumY2
     *          sum of the square of the user/item preference values, over the second item/user
     * @param sumXYdiff2
     *          sum of squares of differences in X and Y values
     * @return similarity value between -1.0 and 1.0, inclusive, or {@link Double#NaN} if no similarity can be
     *         computed (e.g. when no items have been rated by both users
     */
    abstract double computeResult(int n, double sumXY, double sumX2,
            double sumY2, double sumXYdiff2);

    @Override
    public double userSimilarity(final long userID1, final long userID2) {
        final DataModel dataModel = getDataModel();
        final PreferenceArray xPrefs = dataModel
                .getPreferencesFromUser(userID1);
        final PreferenceArray yPrefs = dataModel
                .getPreferencesFromUser(userID2);
        final int xLength = xPrefs.length();
        final int yLength = yPrefs.length();

        if (xLength == 0 || yLength == 0) {
            return Double.NaN;
        }

        long xIndex = xPrefs.getItemID(0);
        long yIndex = yPrefs.getItemID(0);
        int xPrefIndex = 0;
        int yPrefIndex = 0;

        double sumX = 0.0;
        double sumX2 = 0.0;
        double sumY = 0.0;
        double sumY2 = 0.0;
        double sumXY = 0.0;
        double sumXYdiff2 = 0.0;
        int count = 0;

        final boolean hasInferrer = inferrer != null;

        while (true) {
            final int compare = xIndex < yIndex ? -1 : xIndex > yIndex ? 1 : 0;
            if (hasInferrer || compare == 0) {
                double x;
                double y;
                if (xIndex == yIndex) {
                    // Both users expressed a preference for the item
                    x = xPrefs.getValue(xPrefIndex);
                    y = yPrefs.getValue(yPrefIndex);
                } else {
                    // Only one user expressed a preference, but infer the other one's preference and tally
                    // as if the other user expressed that preference
                    if (compare < 0) {
                        // X has a value; infer Y's
                        x = xPrefs.getValue(xPrefIndex);
                        y = inferrer.inferPreference(userID2, xIndex);
                    } else {
                        // compare > 0
                        // Y has a value; infer X's
                        x = inferrer.inferPreference(userID1, yIndex);
                        y = yPrefs.getValue(yPrefIndex);
                    }
                }
                sumXY += x * y;
                sumX += x;
                sumX2 += x * x;
                sumY += y;
                sumY2 += y * y;
                final double diff = x - y;
                sumXYdiff2 += diff * diff;
                count++;
            }
            if (compare <= 0) {
                if (++xPrefIndex >= xLength) {
                    if (hasInferrer) {
                        // Must count other Ys; pretend next X is far away
                        if (yIndex == Long.MAX_VALUE) {
                            // ... but stop if both are done!
                            break;
                        }
                        xIndex = Long.MAX_VALUE;
                    } else {
                        break;
                    }
                } else {
                    xIndex = xPrefs.getItemID(xPrefIndex);
                }
            }
            if (compare >= 0) {
                if (++yPrefIndex >= yLength) {
                    if (hasInferrer) {
                        // Must count other Xs; pretend next Y is far away
                        if (xIndex == Long.MAX_VALUE) {
                            // ... but stop if both are done!
                            break;
                        }
                        yIndex = Long.MAX_VALUE;
                    } else {
                        break;
                    }
                } else {
                    yIndex = yPrefs.getItemID(yPrefIndex);
                }
            }
        }

        // "Center" the data. If my math is correct, this'll do it.
        double result;
        if (centerData) {
            final double meanX = sumX / count;
            final double meanY = sumY / count;
            // double centeredSumXY = sumXY - meanY * sumX - meanX * sumY + n * meanX * meanY;
            final double centeredSumXY = sumXY - meanY * sumX;
            // double centeredSumX2 = sumX2 - 2.0 * meanX * sumX + n * meanX * meanX;
            final double centeredSumX2 = sumX2 - meanX * sumX;
            // double centeredSumY2 = sumY2 - 2.0 * meanY * sumY + n * meanY * meanY;
            final double centeredSumY2 = sumY2 - meanY * sumY;
            result = computeResult(count, centeredSumXY, centeredSumX2,
                    centeredSumY2, sumXYdiff2);
        } else {
            result = computeResult(count, sumXY, sumX2, sumY2, sumXYdiff2);
        }

        if (!Double.isNaN(result)) {
            result = normalizeWeightResult(result, count, cachedNumItems);
        }
        return result;
    }

    @Override
    public final double itemSimilarity(final long itemID1, final long itemID2) {
        final DataModel dataModel = getDataModel();
        final PreferenceArray xPrefs = dataModel.getPreferencesForItem(itemID1);
        final PreferenceArray yPrefs = dataModel.getPreferencesForItem(itemID2);
        final int xLength = xPrefs.length();
        final int yLength = yPrefs.length();

        if (xLength == 0 || yLength == 0) {
            return Double.NaN;
        }

        long xIndex = xPrefs.getUserID(0);
        long yIndex = yPrefs.getUserID(0);
        int xPrefIndex = 0;
        int yPrefIndex = 0;

        double sumX = 0.0;
        double sumX2 = 0.0;
        double sumY = 0.0;
        double sumY2 = 0.0;
        double sumXY = 0.0;
        double sumXYdiff2 = 0.0;
        int count = 0;

        // No, pref inferrers and transforms don't apply here. I think.

        while (true) {
            final int compare = xIndex < yIndex ? -1 : xIndex > yIndex ? 1 : 0;
            if (compare == 0) {
                // Both users expressed a preference for the item
                final double x = xPrefs.getValue(xPrefIndex);
                final double y = yPrefs.getValue(yPrefIndex);
                sumXY += x * y;
                sumX += x;
                sumX2 += x * x;
                sumY += y;
                sumY2 += y * y;
                final double diff = x - y;
                sumXYdiff2 += diff * diff;
                count++;
            }
            if (compare <= 0) {
                if (++xPrefIndex == xLength) {
                    break;
                }
                xIndex = xPrefs.getUserID(xPrefIndex);
            }
            if (compare >= 0) {
                if (++yPrefIndex == yLength) {
                    break;
                }
                yIndex = yPrefs.getUserID(yPrefIndex);
            }
        }

        double result;
        if (centerData) {
            // See comments above on these computations
            final double n = count;
            final double meanX = sumX / n;
            final double meanY = sumY / n;
            // double centeredSumXY = sumXY - meanY * sumX - meanX * sumY + n * meanX * meanY;
            final double centeredSumXY = sumXY - meanY * sumX;
            // double centeredSumX2 = sumX2 - 2.0 * meanX * sumX + n * meanX * meanX;
            final double centeredSumX2 = sumX2 - meanX * sumX;
            // double centeredSumY2 = sumY2 - 2.0 * meanY * sumY + n * meanY * meanY;
            final double centeredSumY2 = sumY2 - meanY * sumY;
            result = computeResult(count, centeredSumXY, centeredSumX2,
                    centeredSumY2, sumXYdiff2);
        } else {
            result = computeResult(count, sumXY, sumX2, sumY2, sumXYdiff2);
        }

        if (!Double.isNaN(result)) {
            result = normalizeWeightResult(result, count, cachedNumUsers);
        }
        return result;
    }

    @Override
    public double[] itemSimilarities(final long itemID1, final long[] itemID2s) {
        final int length = itemID2s.length;
        final double[] result = new double[length];
        for (int i = 0; i < length; i++) {
            result[i] = itemSimilarity(itemID1, itemID2s[i]);
        }
        return result;
    }

    final double normalizeWeightResult(final double result, final int count,
            final int num) {
        double normalizedResult = result;
        if (weighted) {
            final double scaleFactor = 1.0 - (double) count
                    / (double) (num + 1);
            if (normalizedResult < 0.0) {
                normalizedResult = -1.0 + scaleFactor
                        * (1.0 + normalizedResult);
            } else {
                normalizedResult = 1.0 - scaleFactor * (1.0 - normalizedResult);
            }
        }
        // Make sure the result is not accidentally a little outside [-1.0, 1.0] due to rounding:
        if (normalizedResult < -1.0) {
            normalizedResult = -1.0;
        } else if (normalizedResult > 1.0) {
            normalizedResult = 1.0;
        }
        return normalizedResult;
    }

    @Override
    public final void refresh(final Collection<Refreshable> alreadyRefreshed) {
        super.refresh(alreadyRefreshed);
        refreshHelper.refresh(alreadyRefreshed);
    }

    @Override
    public final String toString() {
        return this.getClass().getSimpleName() + "[dataModel:" + getDataModel()
                + ",inferrer:" + inferrer + ']';
    }

}
