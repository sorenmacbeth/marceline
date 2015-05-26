![Marceline The Vampire Queen](http://upload.wikimedia.org/wikipedia/en/4/48/Adventure_Time_-_Marceline.png)

# Marceline

Marceline is a Clojure DSL for [Trident](https://github.com/nathanmarz/storm/wiki/Trident-tutorial).

**Contents**

* [Overview](#overview)
* [Installation](#installation)
* [Streams](#streams)
* [Functions](#functions)
* [Grouping and Partitioning Streams](#grouping)
* [Project](#project)
* [Aggregations and State](#aggregations)
* [DRPC Topologies](#drpc)
* [Parallelism and Tuning](#parallelism)
* [Metrics](#metrics)
* [Reading](#reading)
* [Terminology](#terminology)
* [Support](#support)
* [License](#license)

<a name="overview">
## Overview

Trident provides a layer of abstraction over [Storm](http://storm-project.net/) that allows for stateful stream processing and distributed querying.

Marceline provides a DSL that allows you to define all of the primitives that Trident has to offer from Clojure.

Ready? Grab your willing vessel, and let's do this!

<a name="installation">
## Installation

Marceline is available from clojars. Add the following to your project's `deps`.

```
[yieldbot/marceline "0.2.1"]
```

Note that marceline is pegged to versions of clojure which are compatible with Storm's clojure version; currently that is `1.5.1`.

<a name="streams">
## Streams

Trident topologies start with streams. A stream is an input source for the topology that emits batches of tuples for processing into the topology, by reading from a [spout](#terminology).

In this example, we're using a `FixedBatchSpout` that will emit an infinite stream of sentences:

```clojure
(ns com.black.magic.level-eight-evil-topology
  (:require [marceline.storm.trident :as t])
  (:import [storm.trident.testing FixedBatchSpout]))

(defn mk-fixed-batch-spout [max-batch-size]
  (FixedBatchSpout.
   ;; Name the tuples that the spout will emit.
   (t/fields "sentence")
   max-batch-size
   (into-array (map t/values '("lord ogdoad"
                               "master of level eight shadow world"
                               "the willing vessel offers forth its pure essence")))))
```

This function returns a [spout](#terminology), that can be used to create a new [stream](#terminology) for the topology.

You can add this stream to your topology by calling that function along with  Marcie's `new-stream` function like so:

```clojure
(import '[storm.trident TridentTopology])

(defn build-topology []
  (let [trident-topology (TridentTopology.)
        spout (doto (mk-fixed-batch-spout 3)
                (.setCycle true))]
      (t/new-stream trident-topology "word-counts" spout)))
```

Once you've done that, new `sentence` tuples will be emitted into the topology.

<a name="functions">
## Functions

Trident functions accept tuples from streams or other functions as input, and emit new tuples into the topology
after performing some processing on them:

```clojure
(require '[clojure.string :as string :only [split]])

(t/deftridentfn split-args
  [tuple coll]
  (when-let [args (t/first tuple)]
    (let [words (string/split args #" ")]
      (doseq [word words]
        (t/emit-fn coll word)))))
```

`deftridentfn` accepts a tuple, and the `AppendCollector` for your topology. `deftridentfn` defines a Trident [function](#terminology) `split-args` that takes a tuple, and emits a new tuple into the topology for each `word` in the sentence
by calling `emit-fn` on the `AppendCollector` that gets passed into the function.

Here, we add the `split-args` function we just defined for each `sentence` tuple emitted into the topology, and define the output field as `word`:

```clojure
(defn build-topology []
  (let [trident-topology (TridentTopology.)
        spout (doto (mk-fixed-batch-spout 3)
                (.setCycle true))]
    (-> (t/new-stream trident-topology "word-counts" spout)
        (t/each ["sentence"]
                split-args
                ["word"]))))
```

<a name="project">
## Project

`project` keeps only the [fields](#terminology) you specify from being emitted further into the topology. If your stream consists of the fields `args` and `word`, when you call `(t/project ["word"])`
the output stream only contains the `word` field.

<a name="grouping">
## Grouping and Partitioning Streams

Marceline allows you to group and partition streams of tuples. In our `level-eight-evil-topology` we'll want to group this stream after splitting each word out into its own tuple, so we can perform aggregations on it later.

<a name="group-by">
### Group by

```clojure
(defn build-topology []
  (let [trident-topology (TridentTopology.)
        spout (doto (mk-fixed-batch-spout 3)
                (.setCycle true))]
    (-> (t/new-stream trident-topology "word-counts" spout)
        (t/each ["sentence"]
                split-args
                ["word"])
        ;; Group this stream by `word`
        (t/group-by ["word"]))))
```

<a name="repartitioning-operations">
### Repartitioning Operations

[Repartitoning operations](https://github.com/nathanmarz/storm/wiki/Trident-API-Overview#repartitioning-operations) that Marceline supports:

* `shuffle`: uses a random round robin algorithm to evenly redistribute tuples across all target [partitions](#terminology).
* `batch-global`: All tuples in the batch are sent to the same partition. Different batches in the stream may go to different partitions.
* `partition-by`: takes in a set of fields and does semantic partitioning based on that set of fields. The fields are hashed and modded by the number of target partitions to select the target partition. partitionBy guarantees that the same set of fields always goes to the same target partition.
* `group-by`: repartitions the stream by doing a `partitionBy` on the specified fields, and then within each partition groups tuples together whose group fields are equal.
* `broadcast`: Every tuple is replicated to all target partitions.

<a name="aggregations">
## Aggregations and State

In our `level-eight-evil-topology`, we've split sentences into tuples, grouped them by word, and now we want to count them. Marceline provides `defcombineraggregator` for you to define an aggregation.

### `defcombineraggregator`

```clojure
(t/defcombineraggregator
  count-words
  ([] 0)
  ([tuple] 1)
  ([t1 t2] (+ t1 t2)))
```

`defcombineraggregator` takes a name for the combiner, and three function arities. The first function arity is what is returned if there are no tuples in the partition. In this case, 0. The second function arity is run on each input tuple, and the third arity is used to combine values until there is only one value left.

We'll use our `count-words` function in the next section.

### `persistent-aggregate`

To store these word counts, we need to update a source of state. `persistent-aggregate` takes a [state](#terminology) factory as its first argument. In this case, we'll use one provided for us in the `storm.trident.testing` namespace to store the results of these counts in memory while the topology is running. `MemoryMapState` stores data behind the scenes in a `java.util.concurrent.ConcurrentHashMap` that we can use to simulate a persistent k/v store.

```clojure
(import '[storm.trident.testing MemoryMapState$Factory])

(defn build-topology []
  (let [word-state-factory (MemoryMapState$Factory.)
        trident-topology (TridentTopology.)
        spout (doto (mk-fixed-batch-spout 3)
                (.setCycle true))]
    (-> (t/new-stream trident-topology "word-counts" spout)
        (t/parallelism-hint 16)
        (t/each ["sentence"]
                split-args
                ["word"])
        (t/group-by ["word"])
        ;; Here, we persist our counts of words to our in-memory state.
        (t/persistent-aggregate word-state-factory
                                ["word"]
                                count-words
                                ["count"]))))
```

In this example, we're using the `group-by ["word"]` to tell trident to update a `MapState` where the keys are words and the values are the counts of those words. You can write custom state updaters, state factories and query other sources of state using Marceline.

<a name="drpc">
## DRPC Topologies

Now that we're storing state, we need a way to query our topology. To do that, we're going to create a [DRPC](#terminology) stream in addition to our regular word-count topology. We want Marceline to return counts of words that we ask for, based on the words that have been processed by the `word-counts` topology, and stored in our `MemoryMapState`.

In our `level-eight-evil-topology`, we'll be creating a `LocalDRPC`, and querying our stateful topology in-process using Marceline's `state-query`. For remote topology submission, just use `nil` instead of the `LocalDRPC`.

```clojure
(import '[backtype.storm LocalDRPC]
        '[storm.trident.operation.builtin MapGet])

(defn build-topology [spout drpc]
  (let [word-state-factory (MemoryMapState$Factory.)
       trident-topology (TridentTopology.)
        ;; Here we build our usual word count topology
        word-counts (-> (t/new-stream trident-topology "word-counts" spout)
                        (t/each ["sentence"]
                                split-args
                                ["word"])
                        (t/group-by ["word"])
                        (t/persistent-aggregate word-state-factory
                                                ["word"]
                                                count-words
                                                ["count"]))]
    ;; Now we create our new DRPC stream to produce the sum of counts.
    (-> (t/drpc-stream trident-topology "words" drpc)
        (t/each ["args"]
                split-args
                ["word"])
        (t/project ["word"])
        (t/group-by ["word"])
        ;; here we use `state-query` to query the topology we created above.
        (t/state-query word-counts
                       ["word"]
                       (MapGet.)
                       ["count"]))
    trident-topology))
```

To use `state-query`, we need to pass it a source of state. In this case, we're using the `TridentState` returned by the `persistent-aggregate` function as our source of state. We pass `state-query` the name of the field that we're querying on `["word"]`, and a built-in Trident operation `MapGet`, that will emit the count for each word.

### Querying the DRPC topology

Now we need a way to start our topology, submit some words to count, and query using DRPC. In this example, we're using the `mk-fixed-batch-spout` fn that we defined earlier, and
the `build-topology` function above.

```clojure
(import '[backtype.storm LocalCluster])

(defn run-local! [drpc-words]
  (let [cluster (LocalCluster.)
        local-drpc (LocalDRPC.)
        spout (doto (mk-fixed-batch-spout 3)
                (.setCycle true))]
    (.submitTopology cluster "wordcounter"
                     {}
                     (.build
                      (build-topology
                       spout
                       local-drpc)))
    (Thread/sleep 10000)
    (let [results (.execute local-drpc "words" drpc-words)]
        (.shutdown cluster)
        results)))
```

You can run this function and see it's outputs like this:

```clojure
(def results (run-local! "evil vessel ogdoad"))
;; storm will output a lot of messages
results
;; ==> [["evil", null] ["vessel", 172] ["ogdoad", 172]]

```

**evil** is null because it doesn't appear in the the [fixed batch spout's array of values](#streams).  **vessel** and **ogdoad** do appear and thus they have a count.  The specific count will likely be slightly different on every run.

<a name="parallelism">
## Parallelism and Tuning

Understanding parallelism in Trident can be tricky. If you're not familiar with the concept in Storm or Trident, I suggest you read [Understanding the parallelism of a Storm topology](https://github.com/nathanmarz/storm/wiki/Understanding-the-parallelism-of-a-Storm-topology) first, and then have a look at [Phillip Kromer's gist](https://gist.github.com/mrflip/5958028).

Marceline provides the `paralellism-hint` function, which allows you to set the parallelism of a stream, which can have different effects, depending on where you invoke it in the definition of your topology:

```clojure
(ns com.black.magic.level-eight-evil-topology
  (:require [marceline.storm.trident :as t])
  (:import [storm.trident.TridentTopology]))

(defn build-topology []
  (let [trident-topology (TridentTopology.)
        spout (doto (mk-fixed-batch-spout 3)
                (.setCycle true))]
    (-> (t/new-stream trident-topology "word-counts" spout)
        (t/parallelism-hint 16)
        (t/each ["sentence"]
                split-args
                ["word"])
        ;; Group this stream by `word`
        (t/group-by ["word"]))))
```

Here we're setting the `parallelism-hint` to 16, after we call `new-stream` our topology, telling Trident to create 16 [spouts](#terminology) for this stream.

<a name="metrics">
## Metrics
Storm provides an internal metrics system, which is used for reporting metrics in the Storm UI, but can also be used by topologies to report application metrics. For more information, see this [Endgame blog post](http://www.endgame.com/blog/storm-metrics-how-to.html) or the [Storm wiki page](https://github.com/nathanmarz/storm/wiki/Metrics). Note that marceline's metrics (like Storm's metrics) are meant for both trident as well as vanilla storm topologies. This documentation assumes a familiarity with Storm's metric system.

An example instrumented version of the wordcount topology is [available](https://github.com/yieldbot/marceline/blob/master/test/marceline/topology/wordcount_metrics_test.clj).

For the adventurous, the metrics wrapping source lives in [marceline.storm.metrics](https://github.com/yieldbot/marceline/blob/master/src/clj/marceline/storm/metrics.clj).

### Metrics Consumers
Use the `defmetricsconsumer` macro to define custom metrics. `defmetricsconsumer` takes the name of your consumer, an optional hash-map of options, and the body of the consumer implementation. While `defmetricsconsumer` will generate a consumer class, the value will be a generated function that will manipulate the topology configuration. For example, consider `my-special-consumer` below.

```clojure
(defmetricsconsumer my-special-consumer
  ([this conf registation-arg context error-reporter]
     (println "special consumer initializes!"))
  ([this task-info data-points]
     (println "special consumer handles data!"))
  ([this]
     (println "special consumer cleans up!")))
```

The last implemented method, for cleanup, may be omitted and default to a no-op. The first two methods are required.

Now we use `my-special-consumer` when we construct the topology configuration map. Pass the existing conf into your consumer, like this:

```clojure
(my-special-consumer {TOPOLOGY-WORKERS 6})
```

Note that this consumer registration is safe to do with multiple consumers; they will each add themselves without removing or overwriting others.

To use the registration argument, pass a second argument to your consumer fn. Say, `(my-special-consumer {} "my-registration-argument")`; this will end up being the registration argument passed to the consumer's initialization method.

For consumers that need some state, consumers generated by `defmetricsconsumer` have a `state` field which is an atom. The consumer below sets the state to the configuration, so that could be available for handling data.

```clojure
(defmetricsconsumer my-stateful-consumer
  ([this conf registation-arg context error-reporter]
     (reset! (.state this) conf))
  ([this task-info data-points]
     (println "stateful consumer has conf:" @(.state this))))
```


### Reporting Metrics
Reporting metrics is easy using the `with-metrics` macro. It takes the topology context, reporting periodicity (in seconds), bindings for metrics, and the body of the component.

Consider this example, which reports every minute.

```clojure
(with-metrics context 60
  [wrds :count
   m-wrds :multi-count
   inc-metric (defmetric 0 inc)]
  (...
   (m-wrds "multi-key-to-increment")
   (m-wrds :keywords-work-too)
   (wrds)
   (inc-metric)
   ...))
```
This macro binds the symbols, which will also be used as the `name` when registering with Storm, to values that are functions for updating metrics. The bound metrics are automatically defined and registered.

Using `:count` or `:multi-count` specifies Storm's builtin `CountMetric` or `MultiCountMetric`, respectively. The value may also be a custom metric defined with `defmetric`, as above. The example `defmetric` is a re-implementation of `CountMetric`. The arguments to `defmetric` are simply an initial value and function to be used for updating the value.

<a name="reading">
## Reading
There have been several blog posts related to marceline. Please PR to add any posts that aren't already here!

+ ["Marceline, the finer points"](http://derek.troywest.com/articles/finer-points-marceline/) by @d-t-w
+ ["Marceline's Instruments"](http://yieldbot.com/blog/marcelines-instruments) by @strongh
+ ["Say Hello to Marceline"](http://yieldbot.com/blog/say-hello-to-marceline/) by @sorenmacbeth


<a name="terminology">
## Terminology

* **spout**: A spout emits tuples into the topology, for more information see [Trident Spouts](http://storm.apache.org/documentation/Trident-spouts.html).
* **function**: A Trident function takes in a set of input fields, and emits zero or more tuples as output. See the [Trident docs](http://storm.apache.org/documentation/Trident-API-Overview.html) for more information.
* **stream**: A stream is an unending sequence of batches that are emitted from a spout.
* **field**: A field is a named tuple in a trident topology. Streams can consist of multiple fields.
* **batch**: Tuples are emitted in batches into the topology, for more information see the batching section of the [Trident tutorial](http://storm.apache.org/documentation/Trident-state.html)
* **partition**: Tuples in a Trident batch can be partitioned into logical, or randomly distributed subsets of batches of tuples for distribution to workers in a topology.
* **state**: Trident and Marceline provide support for reading and writing the result of processing to sources of state. The state can be internal to the topology (in memory), or stored externally in a database.
* **DRPC**: DRPC stands for Distributed Remote Procedure Call. For more info, see [Distributed RPC](http://storm.apache.org/documentation/Distributed-RPC.html).


<a name="support">
## Support

YourKit

<img src="http://www.yourkit.com/images/yklogo.png"></img>

YourKit has generously supplied an open source license for their profiler
to improve the performance of Marceline.

YourKit supports open source projects with its full-featured Java
Profiler.  YourKit, LLC is the creator of <a
href="http://www.yourkit.com/java/profiler/index.jsp">YourKit Java
Profiler</a> and <a
href="http://www.yourkit.com/.net/profiler/index.jsp">YourKit .NET
Profiler</a>, innovative and intelligent tools for profiling Java and
.NET applications.

<a name="license">
## License

Copyright © 2014,2015 Yieldbot, Inc.

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
