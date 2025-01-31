# Chapter 4 - Stream Joins

Contents:

* Explanation of join types in Flink.
* Hands-on implementation of stream joins with real-time examples.

## Join Types in Flink

Flink supports several types of **joins** for combining streams or tables based on keys:

1. **Inner Join** – Matches records from both inputs where the key exists in both.
2. **Left Outer Join** – Includes all records from the left stream; unmatched records from the right are null.
3. **Right Outer Join** – Includes all records from the right stream; unmatched records from the left are null.
4. **Full Outer Join** – Includes all records from both streams; unmatched records are filled with nulls.
5. **Interval Join** – Joins two keyed streams based on a time range condition.
6. **Temporal Join** – Joins a stream with a changing table (temporal table) based on event time.
7. **Cross Join (Cartesian Product)** – Pairs every record from one stream with every record from another (rarely used
   due to high cost).

Each join type is optimized for different real-time analytics and event-driven applications.

## Example

In pyflink the joins have to be manually implemented using lower level Process Function API.

First, we have to unify our data stream types, then we will define the key upon which we will join our events, and
finally, we need to define the join function.

```python
joined_stream = stream.map(lambda e: e.__dict__)
.union(stats_stream.map(lambda s: {"sex": s[0], "mean": s[1]}))
.key_by(lambda e: e["sex"])
.process(Join())
```

The `Join` class has to define the `process_element` method and typically define the state:

```python
class Join(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        pass

    def process_element(self, value: Dict, ctx: 'KeyedProcessFunction.Context'):
        pass
```

In our example we will store the previously computed statistics and collected events:

```python
def open(self, runtime_context: RuntimeContext):
    stats_descriptor = ValueStateDescriptor(
        "stats", Types.PICKLED_BYTE_ARRAY()
    )
    events_descriptor = ListStateDescriptor(
        "events", Types.PICKLED_BYTE_ARRAY()
    )
    self.stats = runtime_context.get_state(stats_descriptor)
    self.events = runtime_context.get_list_state(events_descriptor)
```

The `process_element` function will process the events and computed statistics. If the statistic is not yet available,
we temporarily put it in the state. When we receive a statistic, all matching events can be emitted:

```python
def process_element(self, value: Dict, ctx: 'KeyedProcessFunction.Context'):
    if "mean" in value:
        self.stats.update(value["mean"])
        events = list(self.events.get())
        if len(events) > 0:
            for e in events:
                e["mean"] = value["mean"]
                yield e
    else:
        if self.stats.value() is None:
            self.events.add(value)
        else:
            value["mean"] = self.stats.value()
            yield value
```