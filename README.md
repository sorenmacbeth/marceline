![Marceline The Vampire Queen](http://upload.wikimedia.org/wikipedia/en/4/48/Adventure_Time_-_Marceline.png)

# Marceline

Marceline is a Clojure DSL for [Trident](https://github.com/nathanmarz/storm/wiki/Trident-tutorial).

**Contents**

* [Overview](#overview)
* [Installation](#installation)
* [Terminology](#terminology)

<a name="overview">
## Overview

Trident provides a layer of abstraction over [Storm](http://storm-project.net/) that allows forstateful stream processing and distributed querying.

Marceline provides a DSL that allows you to define all of the primitives that Trident has to offer from clojure.

<a name="installation">
## Installation

**TODO: provide lein installation instructions once an artifact is produced**

<a name="streams">
## Streams

Trident topologies start with streams. A stream is an input source for the topology that emits batches of tuples for processing into the topology, by reading from a [spout](#terminology).

In this example, we're using a `FixedBatchSpout` that will emit an infinite stream of sentences:

```clojure
(ns com.black.magic.level-eight-evil-topology
  (:require [marceline.storm.trident :as t]))
            
(defn mk-fixed-batch-spout [max-batch-size]
  (FixedBatchSpout.
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

(let [trident-topology (TridentTopology.)
      spout (doto (mk-fixed-batch-spout 3)
              (.setCycle true))]
    (t/new-stream trident-topology "word-counts" spout))
```

<a name="terminology">
## Terminology

* **spout**: A spout emits tuples into the topology, for more information see [Trident Spouts](https://github.com/nathanmarz/storm/wiki/Trident-spouts).
* **stream**: A stream is an unending sequence of batches that are emitted from a spout.
