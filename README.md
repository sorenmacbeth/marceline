![Marceline The Vampire Queen](http://upload.wikimedia.org/wikipedia/en/4/48/Adventure_Time_-_Marceline.png)

# Marceline

Marceline is a Clojure DSL for [Trident](https://github.com/nathanmarz/storm/wiki/Trident-tutorial).

**Contents**

* [Overview](#overview)
* [Installation](#installation)
* [Creating a topology](#topologies)
* [Streams](#streams)
* [Functions](#functions)
* [Terminology](#terminology)

<a name="overview">
## Overview

Trident provides a layer of abstraction over [Storm](http://storm-project.net/) that allows forstateful stream processing and distributed querying.

Marceline provides a DSL that allows you to define all of the primitives that Trident has to offer from clojure.

<a name="installation">
## Installation

**TODO: provide lein installation instructions once an artifact is produced**

<a name="topologies">
## Creating a topology

A Trident topology usually consists of three basic parts, a [stream](#terminology)

<a name="streams">
## Streams

Trident topologies start with streams. A stream is an input source for the topology that emits batches of tuples for processing into the topology, by reading from a [spout](#terminology).

In this example, we're using a `FixedBatchSpout` that will emit an infinite stream of sentences:

```clojure
(ns com.black.magic.level-eight-evil-topology
  (:require [marceline.storm.trident :as t]))

(defn mk-fixed-batch-spout [max-batch-size]
  (FixedBatchSpout.
   ;; Name the tuples that the spout will emit.
   (t/fields "sentence")
   max-batch-size
   (into-array (map t/values '("lord ogdoad"
                               "master of level eight shadow world"
                               "the willing vessel offers forth it's pure essence")
```

This function returns a [spout](#terminology), that can be used to create a new [stream](#terminology) for the topology.

You can add this stream to your topology by calling that function along with marcie's `new-stream` function like so:

```clojure
(ns com.black.magic.level-eight-evil-topology
  (:require [marceline.storm.trident :as t])
  (:import [storm.trident.TridentTopology]))

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
(ns com.black.magic.level-eight-evil-topology
 (:require [marceline.storm.trident :as t]
           [clojure.string :as string :only [split]]))

(t/deftridentfn split-args
  [tuple coll]
  (when-let [args (t/first tuple)]
    (let [words (string/split args #" ")]
      (doseq [word words]
        (t/emit-fn coll word)))))
```

`deftridentfn` accepts a tuple, and the `AppendCollector` for your topology. `deftridentfn` defines a function
`split-args` that takes a tuple, and emits a new tuple into the topology for each 'word' in the sentence.

Here, we add the `split-args` function for each `sentence` tuple emitted into the topology, and define the
output tuple as `word`:

```clojure
(ns com.black.magic.level-eight-evil-topology
 (:require [marceline.storm.trident :as t]))

(defn build-topology []
  (let [trident-topology (TridentTopology.)
        word-counts (-> (t/new-stream trident-topology "word-counts" spout))
        spout (doto (mk-fixed-batch-spout 3)
                (.setCycle true))]
    (t/each ["sentence"]
            split-args
            ["word"])))
```

<a name="terminology">
## Terminology

* **spout**: A spout emits tuples into the topology, for more information see [Trident Spouts](https://github.com/nathanmarz/storm/wiki/Trident-spouts).
* **stream**: A stream is an unending sequence of batches that are emitted from a spout.
* **batch**: Tuples are emitted in batches into the topology, for more information see the batching section of the [Trident tutorial](https://github.com/nathanmarz/storm/wiki/Trident-state#transactional-spouts)
