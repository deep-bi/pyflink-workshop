import logging
import sys
import inspect
from typing import Tuple, Dict

from pyflink.common import Types, Time
from pyflink.datastream import StreamExecutionEnvironment, KeyedProcessFunction, RuntimeContext, TimerService
from pyflink.datastream.connectors import FileSource, StreamFormat
from pyflink.common.watermark_strategy import WatermarkStrategy
from dataclasses import dataclass

from pyflink.datastream.state import ValueStateDescriptor, ListStateDescriptor

DATA_PATH = '../data/german-credit-headless.csv'
CSV_HEADER = ['index', 'age', 'sex', 'job', 'housing', 'saving_accounts',
              'checking_account', 'credit_amount', 'duration', 'purpose']


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


@dataclass
class Statistics:
    key: str
    total: int
    count: int

    def mean(self) -> Tuple[str, float]:
        return self.key, self.total / self.count

    def __add__(self, other: 'Statistics') -> 'Statistics':
        return Statistics(self.key, self.total + other.total, self.count + other.count)


INTEGER_PARAMS = [p.name for p in inspect.signature(Event).parameters.values() if p.annotation == int]


def json_to_event(record: dict) -> Event:
    for name in INTEGER_PARAMS:
        record[name] = int(record[name])

    return Event(**record)


def csv_line_to_json(line: str) -> dict:
    return dict(zip(CSV_HEADER, line.split(',')))


class Join(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        stats_descriptor = ValueStateDescriptor(
            "stats", Types.PICKLED_BYTE_ARRAY()
        )
        events_descriptor = ListStateDescriptor(
            "events", Types.PICKLED_BYTE_ARRAY()
        )
        self.stats = runtime_context.get_state(stats_descriptor)
        self.events = runtime_context.get_list_state(events_descriptor)

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


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source = FileSource.for_record_stream_format(StreamFormat.text_line_format(), DATA_PATH).build()
    stream = env.from_source(source, WatermarkStrategy.for_monotonous_timestamps(), f"Read: {DATA_PATH}") \
        .map(csv_line_to_json).name("CSV to JSON") \
        .map(json_to_event).name("JSON to Event")

    stats_stream = stream \
        .map(lambda event: Statistics(event.sex, event.credit_amount, 1)) \
        .key_by(lambda stats: stats.key) \
        .reduce(lambda left, right: left + right) \
        .map(Statistics.mean)

    joined_stream = stream.map(lambda e: e.__dict__) \
        .union(stats_stream.map(lambda s: {"sex": s[0], "mean": s[1]})) \
        .key_by(lambda e: e["sex"]) \
        .process(Join())

    joined_stream.print()
    env.execute("Chapter 3")


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    main()
