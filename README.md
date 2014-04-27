Elasticsearch Taste Plugin
=======================

## Overview

Elasticsearch Taste Plugin is [Mahout Taste](https://mahout.apache.org/users/recommender/recommender-documentation.html "Mahout Taste")-based Collaborative Filtering implementation.
This plugin provides the following features on Elasticsearch:

* Data management for Users/Items/Preferences.
* Item-based Recommender.
* User-based Recommender.

## Version

| Taste     | Elasticsearch |
|:---------:|:-------------:|
| master    | 1.1.X         |

### Issues/Questions

Please file an [issue](https://github.com/codelibs/elasticsearch-taste/issues "issue").

## Installation

### Install Taste Plugin

TBD

    $ $ES_HOME/bin/plugin --install org.codelibs/elasticsearch-taste/0.1.0

## Data management

This plugin manages data of Users, Items and Preferences on Elasticsearch.
These data are stored in each index and type.
By default, they are in:

| Data       | Index | Type       |
|:----------:|:------|:-----------|
| User       | (Any) | user       |
| Item       | (Any) | item       |
| Preference | (Any) | preference |

User index manages id and information about itself.
User index contains the following properties:

| Name       | Type   | Description |
|:----------:|:-------|:------------|
| id         | string | Unique ID. If your system has User ID, use it. |
| user_id    | long   | Unique User ID for this plugin. |
| @timestamp | date   | Created/Updated time. |
| (Any)      | (any)  | You can use other information, such as age, tel,... |

Item index manages an item information and contains:

| Name       | Type   | Description |
|:----------:|:-------|:------------|
| id         | string | Unique ID. If your system has Item ID, use it. |
| item_id    | long   | Unique Item ID for this plugin. |
| @timestamp | date   | Created/Updated time. |
| (Any)      | (any)  | You can use other information, such as price, publish date,... |

Preference index manages values rated by an user for an item.

| Name       | Type   | Description |
|:----------:|:-------|:------------|
| user_id    | long   | Unique User ID for this plugin. |
| item_id    | long   | Unique Item ID for this plugin. |
| value      | float  | Value rated by user\_id for item\_id. |
| @timestamp | date   | Created/Updated time. |

You can create/update/delete the above index and document with Elasticsearch manner. 

### Insert Preference Value

Taste plugin provides an useful API to register a preference value.
If you send it to http://.../{index}/\_taste/event, user\_id and item\_id are generated and then the preference value is inserted.



## User Recommender

## Item Recommender


## Tutorial
