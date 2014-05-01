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

If you want to try this plugin quickly, see [Getting Started](https://github.com/codelibs/elasticsearch-taste#getting-started "Getting Started").

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
| user\_id   | long   | Unique User ID for this plugin. |
| @timestamp | date   | Created/Updated time. |
| (Any)      | (any)  | You can use other information, such as age, tel,... |

Item index manages an item information and contains:

| Name       | Type   | Description |
|:----------:|:-------|:------------|
| id         | string | Unique ID. If your system has Item ID, use it. |
| item\_id   | long   | Unique Item ID for this plugin. |
| @timestamp | date   | Created/Updated time. |
| (Any)      | (any)  | You can use other information, such as price, publish date,... |

Preference index manages values rated by an user for an item.

| Name       | Type   | Description |
|:----------:|:-------|:------------|
| user\_id   | long   | Unique User ID for this plugin. |
| item\_id   | long   | Unique Item ID for this plugin. |
| value      | float  | Value rated by user\_id for item\_id. |
| @timestamp | date   | Created/Updated time. |

You can create/update/delete the above index and document with Elasticsearch manner. 

### Insert Preference Value

Taste plugin provides an useful API to register a preference value.
If you send it to http://.../{index}/\_taste/event, user\_id and item\_id are generated and then the preference value is inserted.

For example, if User ID is "U0001", Item ID is "I1000" and the preference(rating) value is 10.0, the request is below:

    $ curl -XPOST localhost:9200/sample/_taste/event -d '{
      user: {
        id: "U0001"
      },
      item: {
        id: "I1000"
      },
      value: 10.0
    }'

Values of user\_id, item\_id and @timestamp are generated automatically.
They are stored in "sample" index.
The index name can be changed to any name you want.

## User Recommender

### Precompute Recommended Items From Users

Recommended items for an user are computed from similar users.
The computing process is started by creating a river configuration.
If 10 recommended items are generated from similar users, the river configuration is:

    $ curl -XPOST localhost:9200/_river/sample/_meta -d '{
      "type": "taste",
      "action": "recommended_items_from_user",
      "num_of_items": 10,
      "max_duration": 120,
      "data_model": {
        "class": "org.codelibs.elasticsearch.taste.model.ElasticsearchDataModel",
        "scroll": {
          "size": 1000,
          "keep_alive": 60
        },
        "cache": {
          "weight": "100m"
        }
      },
      "index_info": {
        "preference": {
          "index": "sample",
          "type": "preference"
        },
        "user": {
          "index": "sample",
          "type": "user"
        },
        "item": {
          "index": "sample",
          "type": "item"
        },
        "recommendation": {
          "index": "sample",
          "type": "recommendation"
        },
        "field": {
          "user_id": "user_id",
          "item_id": "item_id",
          "value": "value",
          "items": "items",
          "timestamp": "@timestamp"
        }
      },
      "similarity": {
        "factory": "org.codelibs.elasticsearch.taste.similarity.LogLikelihoodSimilarityFactory"
      },
      "neighborhood": {
        "factory": "org.codelibs.elasticsearch.taste.neighborhood.NearestNUserNeighborhoodFactory"
      }
    }'

The configuration is:

| Name | Type | Description |
|:-----|:-----|:------------|
| action | string | The kind of a process. |
| num\_of\_items | int | The number of recommended items. |
| max\_duration | int  | Max duration for computing(min). |
| data\_model.class | string | Class name for DataModel implementation. |
| data\_model.scroll | object | Elasticsearch scroll parameters. |
| data\_model.cache | string | Cache size for the data model. |
| index\_info | object | Index information(index/type/property name). |
| similarity.factory | string | Factroy name for Similarity implementation. |
| neighborhood.factory | string | Factroy name for Neighborhood implementation. |

The recommended items are stored in sample/recommendation.
You can see the result by:

    curl -XGET "localhost:9200/sample/recommendation/_search?q=*:*&pretty"

The value of "items" property is recommended items.

### Evaluate Result

TBD

    $ curl -XPOST localhost:9200/_river/sample/_meta -d '{
      "type": "taste",
      "action": "evaluate_items_from_user",
      "training_percentage": 0.9,
      "evaluation_percentage": 0.1,
      "data_model": {
        "class": "org.codelibs.elasticsearch.taste.model.ElasticsearchDataModel",
        "scroll": {
          "size": 1000,
          "keep_alive": 60
        },
        "cache": {
          "weight": "100m"
        }
      },
      "index_info": {
        "preference": {
          "index": "sample",
          "type": "preference"
        },
        "user": {
          "index": "sample",
          "type": "user"
        },
        "item": {
          "index": "sample",
          "type": "item"
        },
        "report": {
          "index": "sample",
          "type": "report"
        },
        "field": {
          "user_id": "user_id",
          "item_id": "item_id",
          "value": "value",
          "timestamp": "@timestamp"
        }
      },
      "similarity": {
        "factory": "org.codelibs.elasticsearch.taste.similarity.LogLikelihoodSimilarityFactory"
      },
      "neighborhood": {
        "factory": "org.codelibs.elasticsearch.taste.neighborhood.NearestNUserNeighborhoodFactory"
      }
    }'

## Item Recommender

### Precompute Recommended Items From Items

Recommended items for an item are computed from similar items.
The computing process is started by creating a river configuration.
If 10 recommended items are generated from similar items, the river configuration is:

    $ curl -XPOST localhost:9200/_river/sample/_meta -d '{
      "type": "taste",
      "action": "recommended_items_from_item",
      "num_of_items": 10,
      "max_duration": 120,
      "data_model": {
        "class": "org.codelibs.elasticsearch.taste.model.ElasticsearchDataModel",
        "scroll": {
          "size": 1000,
          "keep_alive": 60
        },
        "cache": {
          "weight": "100m"
        }
      },
      "index_info": {
        "preference": {
          "index": "sample",
          "type": "preference"
        },
        "user": {
          "index": "sample",
          "type": "user"
        },
        "item": {
          "index": "sample",
          "type": "item"
        },
        "item_similarity": {
          "index": "sample",
          "type": "item_similarity"
        },
        "field": {
          "user_id": "user_id",
          "item_id": "item_id",
          "value": "value",
          "items": "items",
          "timestamp": "@timestamp"
        }
      },
      "similarity": {
        "factory": "org.codelibs.elasticsearch.taste.similarity.LogLikelihoodSimilarityFactory"
      }
    }'

The recommended items are stored in sample/item\_similarity.
You can see the result by:

    curl -XGET "localhost:9200/sample/item_similarity/_search?q=*:*&pretty"

The value of "items" property is recommended items.

## Getting Started

### Create Data

In this section, using [MovieLens](http://grouplens.org/datasets/movielens/ "MovieLens") data set, you can learn about Taste plugin.
For more information about MovieLens, see [MovieLens](http://grouplens.org/datasets/movielens/ "MovieLens") site.
The preference data set is u.data, it contains user id, item id, rating and timestamp.
Download it and then insert data by [Event API](https://github.com/codelibs/elasticsearch-taste/blob/master/README.md#insert-preference-value "Event API"):

    curl -o u.data http://files.grouplens.org/datasets/movielens/ml-100k/u.data
    cat u.data | awk '{system("curl -XPOST localhost:9200/movielens/_taste/event?pretty -d {\"user\":{\"id\":" $1 "},\"item\":{\"id\":" $2 "},\"value\":" $3 ",\"timestamp\":" $4 "000}")}'

By the above request, the preference values are stored in "movielens" index.
After inserting data, check them in the index:

    curl -XGET "localhost:9200/movielens/_search?q=*:*&pretty"

### Recommend Items

TBD
