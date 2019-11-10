Elasticsearch Taste Plugin
=======================

## 概要

Elasticsearchのプラグインは[Mahout Taste](https://mahout.apache.org/users/recommender/recommender-documentation.html "Mahout Taste")（協調フィルタリングの実装）です。
このプラグインは、Elasticsearchのレコメンドエンジンの次の機能を提供します。

* 利用者/商品/嗜好のデータ管理
* 商品ベースのレコメンダ
* 利用者ベースのレコメンダ
* 類似利用者/コンテンツ
* テキスト解析

## バージョン

| Taste     | Elasticsearch |
|:---------:|:-------------:|
| master    | 1.5.X         |
| 1.5.0     | 1.5.1         |
| 0.4.0     | 1.4.0         |
| 0.3.1     | 1.3.1         |

このプラグインはJava8以上でサポートされていることに注意して下さい。
Elasticsearch Taste 0.4.0.の下の[README_river.md](https://github.com/codelibs/elasticsearch-taste/blob/master/README_river.md "README_river.md")を参照してください。

### 課題/質問

何かあれば、[課題](https://github.com/codelibs/elasticsearch-taste/issues "issue")を報告してください。
(日本語は[ここ](https://github.com/codelibs/codelibs-ja-forum "here")です。)

## インストール

### プラグインのインストール

    $ $ES_HOME/bin/plugin --install org.codelibs/elasticsearch-taste/1.5.0

## 入門

### データの挿入

このセクションでは、[MovieLens](http://grouplens.org/datasets/movielens/ "MovieLens")データセットを使用して、Tasteプラグインについて学ぶことができます。
MovieLensの詳細については[MovieLens](http://grouplens.org/datasets/movielens/ "MovieLens")を見てください。
データセットu.dataを利用します。これには利用者ID、商品ID、評価、タイムスタンプが含まれています。
それをダウンロードし、[Event API](https://github.com/codelibs/elasticsearch-taste/blob/master/README.md#insert-preference-value "Event API")によってデータを挿入して下さい。

    curl -o u.data http://files.grouplens.org/datasets/movielens/ml-100k/u.data
    cat u.data | awk '{system("curl -XPOST localhost:9200/movielens/taste/event?pretty -d \"{\\\"user\\\":{\\\"id\\\":" $1 "},\\\"item\\\":{\\\"id\\\":" $2 "},\\\"value\\\":" $3 ",\\\"timestamp\\\":" $4 "000}\"")}'


上記の要求により、優先度の値は"movielens"インデックスに格納されます。
データを挿入した後、インデックスにあるのを確認してください。

    curl -XGET "localhost:9200/movielens/_search?q=*:*&pretty"

### 利用者から商品をレコメンド

利用者からのレコメンドされた商品を計算するには、次のリクエストを実行します。

    curl -XPOST localhost:9200/taste/action/recommended_items_from_user -d '{
      "num_of_items": 10,
      "data_model": {
        "cache": {
          "weight": "100m"
        }
      },
      "index_info": {
        "index": "movielens"
      }
    }'

計算結果は movielens/recommendation に格納されます。
上記リクエストのレスポンスの"name"プロパティにaction名が含まれます。
計算が終わったかどうかを確認するには、GETリクエストを送信します。

    curl -XGET "localhost:9200/taste/action?pretty"

応答にアクション名が含まれている場合は、リクエストがまだ完了していません。

結果を確認するため、次のクエリを送信します。

    curl -XGET "localhost:9200/movielens/recommendation/_search?q=*:*&pretty"

"user_id"プロパティの値は、対象の利用者です。"items"プロパティは、"item_id"と"values"プロパティを含むレコメンドされた商品です。.
"value"プロパティは、類似の値です。

計算は長い時間がかかるかもしれません…
もし停止したいなら、以下を実行してください。

    curl -XDELETE localhost:9200/taste/action/{action_name}

### 結果の評価

次の"evaluate\_items\_from\_user"アクションでは、類似性やneighborhoodのようなパラメータを評価することが出来ます。

    curl -XPOST localhost:9200/taste/action/evaluate_items_from_user -d '{
      "evaluation_percentage": 1.0,
      "training_percentage": 0.9,
      "margin_for_error": 1.0,
      "data_model": {
        "cache": {
          "weight": "100m"
        }
      },
      "index_info": {
        "index": "movielens"
      },
      "neighborhood": {
        "factory": "org.codelibs.elasticsearch.taste.neighborhood.NearestNUserNeighborhoodFactory",
        "neighborhood_size": 100
      },
      "evaluator": {
        "id": "movielens_result",
        "factory": "org.codelibs.elasticsearch.taste.eval.RMSEvaluatorFactory"
      }
    }'

結果はreportタイプに格納されます。

    curl -XGET "localhost:9200/movielens/report/_search?q=evaluator_id:movielens_result&pretty"
    {
      "_index": "movielens",
      "_type": "report",
      "_id": "COscswwlQIugP2Rf0XKH-w",
      "_version": 1,
      "_score": 1,
      "_source": {
        "evaluation": {
          "score": 1.0199981244413419,
          "preference": {
            "total": 10092,
            "no_estimate": 178,
            "success": 6671,
            "failure": 3243
          },
          "time": {
            "average_processing": 18,
            "total_processing": 184813,
            "max_processing": 22881
          }
          "target": {
            "test": 915,
            "training": 943
          }
        }
        "@timestamp": "2014-05-07T21:15:43.948Z",
        "evaluator_id": "movielens_result",
        "report_type": "user_based",
        "config": {
          "training_percentage": 0.9,
          "evaluation_percentage": 1,
          "margin_for_error": 1
        }
      }
    }

### 類似利用者

類似利用者をを割り当てるには、下のリクエストを実行します。

    curl -XPOST localhost:9200/taste/action/similar_users -d '{
      "num_of_users": 10,
      "data_model": {
        "cache": {
          "weight": "100m"
        }
      },
      "index_info": {
        "index": "movielens"
      }
    }'

利用者ID1の類似利用者を確認する場合は、以下を入力して下さい。

    curl -XGET "localhost:9200/movielens/user_similarity/_search?q=user_id:1&pretty"

### システムIDによって取得

このプラグインは、user_idやitem_idのようなIDを所有しています。
自分のシステムIDにアクセスする場合は、次の検索APIを使用します。

    curl -XGET "localhost:9200/{index}/{type}/taste/{user|item}/{your_user_or_item_id}?pretty"

たとえば、IDが115の類似利用者を取得したい場合は以下のようにします。

    curl -XGET localhost:9200/movielens/recommendation/taste/user/1

### テキストからベクトルを作成

"term_vector"を含むフィールドを持つインデックスから用語ベクターを作成できます。
次の例では、テキストデータは"ap"インデックスの"description"フィールドに格納されています。

    curl -XPOST localhost:9200/ap?pretty
    curl -XPOST localhost:9200/ap/article/_mapping?pretty -d '{
      "article" : {
        "properties" : {
          "id" : {
            "type" : "string",
            "index" : "not_analyzed"
          },
          "label" : {
            "type" : "string",
            "index" : "not_analyzed"
          },
          "description" : {
            "type" : "string",
            "analyzer" : "standard",
            "term_vector": "with_positions_offsets_payloads",
            "store" : true
          }
        }
      }
    }'
    curl -o ap.txt http://mallet.cs.umass.edu/ap.txt
    sed -e "s/[\'\`\\]//g" ap.txt | awk -F"\t" '{sub(" ","",$1);system("curl -XPOST localhost:9200/ap/article/" $1 " -d \"{\\\"id\\\":\\\"" $1 "\\\",\\\"label\\\":\\\"" $2 "\\\",\\\"description\\\":\\\"" $3 "\\\"}\"")}'

用語ベクターを作成するには、下記を実行します。

    curl -XPOST localhost:9200/taste/action/generate_term_values?pretty -d '{
      "source": {
        "index": "ap",
        "type": "article",
        "fields": ["description"]
      },
      "event": {
        "user_index": "ap_term",
        "user_type": "doc",
        "item_index": "ap_term",
        "item_type": "term",
        "preference_index": "ap_term",
        "preference_type": "preference"
      }
    }'

sourceプロパティは、インデックス、タイプ、source情報などのフィールドを指定し、eventプロパティはuser/item/preferenceフォーマットである出力情報です。

## 仕様書

### データ管理

このプラグインは、Elasticsearch上でUsers、Items、Preferencesのデータを管理しています。
これらのデータは、それぞれのindexやtypeに格納されています。
デフォルトでは、以下のようになっています。

| Data       | Index | Type       |
|:----------:|:------|:-----------|
| User       | (Any) | user       |
| Item       | (Any) | item       |
| Preference | (Any) | preference |

Userインデックスは、自身のidと情報を管理しています。
Userインデックスは、以下の属性を含んでいます。

| Name       | Type   | Description |
|:----------:|:-------|:------------|
| system\_id | string | 固有のID。 システムが利用者IDを持つ場合には、これを使います。 |
| user\_id   | long   | このプラグインのための固有の利用者ID。 |
| @timestamp | date   | 作成/更新 された時間。 |
| (Any)      | (any)  | age,tel,…などの他の情報を使用できる。 |

Itemインデックスはitem情報を管理し、以下を含んでいます。

| Name       | Type   | Description |
|:----------:|:-------|:------------|
| system\_id | string | 固有のID。 システムが商品IDを持つ場合には、これを使います。 |
| item\_id   | long   | このプラグインのための固有の商品ID。 |
| @timestamp | date   | 作成/更新 された時間。 |
| (Any)      | (any)  | price, publish date,...などの他の情報を使用できる。 |

Preferenceインデックスは、商品の利用者による評価値を管理します。

| Name       | Type   | Description |
|:----------:|:-------|:------------|
| user\_id   | long   | このプラグインのための固有の利用者ID。 |
| item\_id   | long   | このプラグインのための固有の商品ID。 |
| value      | float  | item_idのuser_idによる評価値。 |
| @timestamp | date   | 作成/更新 された時間。 |

Elasticsearchの作法を使用し、上記のインデックスおよびドキュメントを作成/更新/削除することが出来ます。

#### Preference値の挿入

Tasteプラグインは、preference値を登録するための有用なAPIを提供します。
それを http://.../{index}/\taste/event に送信する場合、user\_idとitem\_idは生成され、preference値は挿入されます。
途中に改行を挟むことで、一度のリクエストで複数個の値を登録することができます。

例えば、利用者IDが"U0001"、商品IDが"I1000"、preference値が10.0のデータを登録する場合、要求は以下の通りです。

    curl -XPOST localhost:9200/sample/taste/event -d '{ user: { id: "U0001" }, item: { id: "I1000" }, value: 10.0 }'

複数個の値を登録する場合、次のように要求します。

    curl -XPOST localhost:9200/sample/taste/event -d \
      '{ user: { id: "U0001" }, item: { id: "I1000" }, value: 10.0 }
      { user: { id: "U0002" }, item: { id: "I1001" }, value: 15.0 }'

user\_id、item\_id、@timestampは自動的に生成されます。
これらは"sample"インデックスに保存されます。
そのインデックス名は任意の名前に変更することが出来ます。

### 利用者レコメンダ

#### 利用者が事前算出したレコメンドされた商品

利用者のレコメンドされた商品は、類似利用者から算出されます。
演算処理は、riverの設定することにより開始されます。
10個のレコメンドされた商品が類似利用者から生成される場合は、riverの構成は次のとおりです。

    curl -XPOST localhost:9200/taste/action/recommended_items_from_user -d '{
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
        "factory": "org.codelibs.elasticsearch.taste.neighborhood.NearestNUserNeighborhoodFactory",
        "min_similarity": 0.9,
        "neighborhood_size": 10
      }
    }'

設定

| Name | Type | Description |
|:-----|:-----|:------------|
| num\_of\_items | int | レコメンドされた商品の数。 |
| max\_duration | int  | コンピューティングのための最大時間(分)。 |
| data\_model.class | string | DataModel実装のためのクラス名。 |
| data\_model.scroll | object | Elasticsearchスクロールパラメータ。 |
| data\_model.cache | string | データモデルのためのキャッシュサイズ。 |
| index\_info | object | インデックス情報（index/type/property name）。 |
| similarity.factory | string | 類似実装のためのFactroy名。 |
| neighborhood.factory | string | Neighborhood実装のためのFactroy名。 |

レコメンドされた商品は、sample/recommendationに格納される。
結果を以下のようにして見ることができる。

    curl -XGET "localhost:9200/sample/recommendation/_search?q=*:*&pretty"

"items"プロパティの値は、レコメンドされた商品です。

#### 結果の評価

レコメンドされた商品を生成するためのパラメータを評価するには、以下の "evaluate\_items\_from\_user" を使用することができます。.

    curl -XPOST localhost:9200/taste/action/evaluate_items_from_user -d '{
      "evaluation_percentage": 0.5,
      "training_percentage": 0.9,
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

| Name | Type | Description |
|:-----|:-----|:------------|
| evaluation\_percentage | float | データモデルのためのサンプリングレート。0.5なら50％のデータを使用。  |
| training\_percentage | float | サンプリングされたデータモデルのトレーニングデータセットに対するレート。0.9の場合、90％のデータは試験データで10％のデータはテスト用です。 |

その結果はreportインデックスに保存されます。(この例はsample/reportです。)

### 商品のレコメンダ

#### 商品から事前算出されたレコメンドされた商品

商品のレコメンドされた商品は、類似商品から算出されます。
その演算処理は、riverの設定をすることにより開始されます。
10個のレコメンドされた商品が類似商品から生成される場合は、riverの構成は以下のとおりです。

    curl -XPOST localhost:9200/taste/action/recommended_items_from_item -d '{
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

そのレコメンドされた商品はsample/item\_similarityに保存されます。
以下のようにして結果を見ることができます。

    curl -XGET "localhost:9200/sample/item_similarity/_search?q=*:*&pretty"

"items"プロパティの値は、レコメンドされた商品です。

