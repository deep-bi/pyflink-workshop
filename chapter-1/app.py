import logging
import sys
import inspect

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FileSource, StreamFormat
from pyflink.common.watermark_strategy import WatermarkStrategy
from dataclasses import dataclass

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


INTEGER_PARAMS = [p.name for p in inspect.signature(Event).parameters.values() if p.annotation == int]


def json_to_event(record: dict) -> Event:
    for name in INTEGER_PARAMS:
        record[name] = int(record[name])

    return Event(**record)


def csv_line_to_json(line: str) -> dict:
    return dict(zip(CSV_HEADER, line.split(',')))


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source = FileSource.for_record_stream_format(StreamFormat.text_line_format(), DATA_PATH).build()
    stream = env.from_source(source, WatermarkStrategy.for_monotonous_timestamps(), f"Read: {DATA_PATH}")

    stream = stream \
        .map(csv_line_to_json).name("CSV to JSON") \
        .map(json_to_event).name("JSON to Event") \
        .filter(lambda event: event.age >= 30 and event.job == 0)

    stream.print()
    env.execute("Chapter 1")


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    main()
