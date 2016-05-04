package org.codelibs.elasticsearch.taste.model;

import java.util.Map;

import org.codelibs.elasticsearch.taste.TasteConstants;
import org.codelibs.elasticsearch.taste.util.SettingsUtils;

public class IndexInfo {
    private String preferenceIndex;

    private String preferenceType;

    private String userIndex;

    private String userType;

    private String itemIndex;

    private String itemType;

    private String recommendationIndex;

    private String recommendationType;

    private String itemSimilarityIndex;

    private String itemSimilarityType;

    private String userSimilarityIndex;

    private String userSimilarityType;

    private String reportIndex;

    private String reportType;

    private String resultIndex;

    private String resultType;

    private String userIdField;

    private String itemIdField;

    private String valueField;

    private String timestampField;

    private String itemsField;

    private String usersField;

    public IndexInfo(final Map<String, Object> indexInfoSettings) {
        final String defaultIndex = SettingsUtils.get(indexInfoSettings,
                "index");

        final Map<String, Object> preferenceSettings = SettingsUtils.get(
                indexInfoSettings, "preference");
        preferenceIndex = SettingsUtils.get(preferenceSettings, "index",
                defaultIndex);
        preferenceType = SettingsUtils.get(preferenceSettings, "type",
                TasteConstants.PREFERENCE_TYPE);

        final Map<String, Object> userSettings = SettingsUtils.get(
                indexInfoSettings, "user");
        userIndex = SettingsUtils.get(userSettings, "index", defaultIndex);
        userType = SettingsUtils.get(userSettings, "type",
                TasteConstants.USER_TYPE);

        final Map<String, Object> itemSettings = SettingsUtils.get(
                indexInfoSettings, "item");
        itemIndex = SettingsUtils.get(itemSettings, "index", defaultIndex);
        itemType = SettingsUtils.get(itemSettings, "type",
                TasteConstants.ITEM_TYPE);

        final Map<String, Object> recommendationSettings = SettingsUtils.get(
                indexInfoSettings, "recommendation");
        recommendationIndex = SettingsUtils.get(recommendationSettings,
                "index", defaultIndex);
        recommendationType = SettingsUtils.get(recommendationSettings, "type",
                TasteConstants.RECOMMENDATION_TYPE);

        final Map<String, Object> itemSimilaritySettings = SettingsUtils.get(
                indexInfoSettings, "item_similarity");
        itemSimilarityIndex = SettingsUtils.get(itemSimilaritySettings,
                "index", defaultIndex);
        itemSimilarityType = SettingsUtils.get(itemSimilaritySettings, "type",
                TasteConstants.ITEM_SIMILARITY_TYPE);

        final Map<String, Object> userSimilaritySettings = SettingsUtils.get(
                indexInfoSettings, "user_similarity");
        userSimilarityIndex = SettingsUtils.get(userSimilaritySettings,
                "index", defaultIndex);
        userSimilarityType = SettingsUtils.get(userSimilaritySettings, "type",
                TasteConstants.USER_SIMILARITY_TYPE);

        final Map<String, Object> reportSettings = SettingsUtils.get(
                indexInfoSettings, "report");
        reportIndex = SettingsUtils.get(reportSettings, "index", defaultIndex);
        reportType = SettingsUtils.get(reportSettings, "type",
                TasteConstants.REPORT_TYPE);

        final Map<String, Object> resultSettings = SettingsUtils.get(
                indexInfoSettings, "result");
        resultIndex = SettingsUtils.get(resultSettings, "index", defaultIndex);
        resultType = SettingsUtils.get(resultSettings, "type",
                TasteConstants.RESULT_TYPE);

        final Map<String, Object> fieldSettings = SettingsUtils.get(
                indexInfoSettings, "field");
        userIdField = SettingsUtils.get(fieldSettings, "user_id",
                TasteConstants.USER_ID_FIELD);
        itemIdField = SettingsUtils.get(fieldSettings, "item_id",
                TasteConstants.ITEM_ID_FIELD);
        valueField = SettingsUtils.get(fieldSettings, "value",
                TasteConstants.VALUE_FIELD);
        timestampField = SettingsUtils.get(fieldSettings, "timestamp",
                TasteConstants.TIMESTAMP_FIELD);
        itemsField = SettingsUtils.get(fieldSettings, "items",
                TasteConstants.ITEMS_FILED);
        usersField = SettingsUtils.get(fieldSettings, "users",
                TasteConstants.USERS_FILED);
    }

    public String getPreferenceIndex() {
        return preferenceIndex;
    }

    public String getPreferenceType() {
        return preferenceType;
    }

    public String getUserIndex() {
        return userIndex;
    }

    public String getUserType() {
        return userType;
    }

    public String getItemIndex() {
        return itemIndex;
    }

    public String getItemType() {
        return itemType;
    }

    public String getRecommendationIndex() {
        return recommendationIndex;
    }

    public String getRecommendationType() {
        return recommendationType;
    }

    public String getItemSimilarityIndex() {
        return itemSimilarityIndex;
    }

    public String getItemSimilarityType() {
        return itemSimilarityType;
    }

    public String getUserSimilarityIndex() {
        return userSimilarityIndex;
    }

    public String getUserSimilarityType() {
        return userSimilarityType;
    }

    public String getReportIndex() {
        return reportIndex;
    }

    public String getReportType() {
        return reportType;
    }

    public String getResultIndex() {
        return resultIndex;
    }

    public String getResultType() {
        return resultType;
    }

    public String getUserIdField() {
        return userIdField;
    }

    public String getItemIdField() {
        return itemIdField;
    }

    public String getValueField() {
        return valueField;
    }

    public String getTimestampField() {
        return timestampField;
    }

    public String getItemsField() {
        return itemsField;
    }

    public String getUsersField() {
        return usersField;
    }
}