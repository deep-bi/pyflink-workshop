# Chapter 1 - Introduction to Flink Stream Processing

Contents:

* Overview of Flink's architecture and core concepts.
* Hands-on examples with connectors, map, and filter transformations.

## Stream execution environment

To write our first application we need to have a **Stream execution environment**.
We can create one by using the following code:

```python
from pyflink.datastream import StreamExecutionEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
```

The environment is used to configure sources, parallelism, restart strategy, and more.
It is provided by the Flink runtime when deploying the job.

If we want our job to execute only with given parallelism then we can use:

```python
env.set_parallelism(1)
```

This means that only one instance of each task will be run. The tasks are executed
by the task executors. You cannot start a job with higher parallelism than there are
task executors (technically you can, but it will fail).

## The source

The source is the starting point of all streams. It has a beginning and doesn't have an end.
Of course, we can have a bounded source, that has an end. However, in general, streaming sources
are unbounded. Examples of streaming sources are:

- Kafka source (or any other queue/pubsub system)
- Periodic file source (scans directory for new files to read)

An example of a bounded source would be a simple file source, that reads the file content once and the file is
static.

In this chapter, we will use a simple file source to
read [German Credit](https://github.com/vibhor98/German-Credit-Dataset/blob/master/german_credit_data.csv) data into a
stream.

To create the file source we will use: `env.from_source` function. However, there are also different
functions like:

- `env.from_collection` - you can create a simple bounded source from Python collection.
- `env.read_text_file` - reads file line by line, should be used only for debugging purposes.

As we now know the differences between different stream creation methods, then we can create our stream:

```python
from pyflink.datastream.connectors import FileSource, StreamFormat
from pyflink.common.watermark_strategy import WatermarkStrategy

DATA_PATH = 'data/german-credit-headless.csv'

source = FileSource.for_record_stream_format(StreamFormat.text_line_format(), DATA_PATH).build()
stream = env.from_source(source, WatermarkStrategy.for_monotonous_timestamps(), f"Read: {DATA_PATH}")
```

To explain the code above: First, we create a source which is a FileSource. In this case, the source reads the file
line by line as a string encoded in `UTF-8`. Then we use that source to create a stream.

## The sink

We can test our application at this stage. All we need to do is to add these lines:

```python
stream.print()
env.execute("Chapter 1")
```

This will print a stream to standard output. The `.print()` is a sink, that materializes the output.
Without this, the stream doesn't work, as there is no consumer of the data, hence, no data is published.

The `.execute` is necessary to execute a job in the first place.
What we did before, is just a description of a DAG that will be executed in the future. That's why the `.execute` is
important to have as without execution we won't be able to run the job in the first place.
The `.execute()` accepts a job name, in this case, we named it `Chapter 1`.

To run the application just type: `python chapter-1/app.py`.
As you can see after running the application it stopped. Why? Streaming applications do not stop as
sources are unbounded. However, in our case the source is bounded - only a single static file needs to be read.
This shows the power of Flink: we can treat batch input as stream input, as the batch is a specific case of a stream.

This unification allows us to have a cleaner API for both approaches for example for processing real-time data
and historical data.

## Stateless transformations

### Parsing the input with a map function

Now lets parse the input data as a Python data class.

First, we need to parse a raw data string to JSON (Python dict):

```python
CSV_HEADER = ['index', 'age', 'sex', 'job', 'housing', 'saving_accounts',
              'checking_account', 'credit_amount', 'duration', 'purpose']


def csv_line_to_json(line: str) -> dict:
    return dict(zip(CSV_HEADER, line.split(',')))
```

Then we can pass the function to a `.map` as follows (between source and sink):

```python
stream = stream.map(csv_line_to_json).name("CSV to JSON")
```

What we did, is to create a stateless transformation with the stream.
We also named this transformation, so it will be human-readable in the graph view of the application.
You can try and run the application to see that the types were converted
correctly. Use `python chapter-1/app.py`.

### Converting to data class

Now let's convert the dictionary into a data class with correct data types (now everything is a string).
First, let's define the data class that describes the records:

```python
from dataclasses import dataclass


@dataclass
class Event:
    index: int
    age: int
    sex: str
    job: int
    housing: int
    saving_accounts: str
    checking_account: str
    credit_amount: int
    duration: int
    purpose: str
```

Now we can create a list of names that should be translated into integers:

```python
import inspect

INTEGER_PARAMS = [p.name for p in inspect.signature(Event).parameters.values() if p.annotation == int]
```

We can now write a function that will be used for converting events into dataclasses:

```python
def json_to_event(record: dict) -> Event:
    for name in INTEGER_PARAMS:
        record[name] = int(record[name])

    return Event(**record)
```

Finally, we can use it in our data pipeline. We will chain multiple operations. Note that we could put everything
into a single function.

```python
stream = stream.map(json_to_event).name("JSON to Event")
```

## Filtering the streams

Now let's imagine we need to process entries about people that are in their thirties and older.
Also, we're interested in people that have at least one job.

To do it, we can use `stream.filter` that accepts a predicate: takes stream value and returns a boolean value.
If `True` then the event is kept, otherwise it is discarded. Also in PyFlink, we can use Python lambdas.
Let's use them in this example:

```
stream = stream.filter(lambda event: event.age >= 30 and event.job > 0)
```

If you run the application you can see that some records were discarded.

## Submitting job to a Flink cluster

To submit a job to a running Flink cluter we need to start an interactive shell on the JobManager and run:

```bash
./bin/flink run --python app.py
```

We can check the result in the [Flink WebUI](http://localhost:8081/).
