import dataclasses
import json
from typing import List

import pandas as pd


@dataclasses.dataclass
class Metadata:
    day: str
    event_type: str
    count: int


@dataclasses.dataclass
class RequestInput:
    from_day: str  # inclusive
    to_day: str  # inclusive
    event_type: List[str]


def read_csv(path: str) -> List[Metadata]:
    import csv
    with open(path, 'r') as f:
        reader = list(csv.reader(f))
        return [
            Metadata(day=line[1], event_type=line[2], count=int(line[3]))
            for line in reader[1::]
        ]


# Output Example 1:
# [
#     {
#         "from_day": "2024-08-01",
#         "to_day": "2024-08-01",
#         event_type": ["LINK_FOLDER_CREATED", "INVITE_TO_FORWRD", "CLICK_RESOURCE_CARD"]
#     },
#     {
#         "from_day": "2024-08-01",
#         "to_day": "2024-08-01",
#         "event_type": ["Unsupported Url (Auto brand recommendation)"]
#     },
#     ....
# ]
#
# Output Example 2:
# [
#     {
#         "from_day": "2024-08-01",
#         "to_day": "2024-08-03",
#         event_type": ["LINK_FOLDER_CREATED", "INVITE_TO_FORWRD", "CLICK_RESOURCE_CARD"]
#     },
#     {
#         "from_day": "2024-08-04",
#         "to_day": "2024-08-10",
#         "event_type": ["LINK_FOLDER_CREATED", "INVITE_TO_FORWRD", "CLICK_RESOURCE_CARD"]
#     },
#     ....
# ]

def recursion(events, length, result, event, dates):
    while length > 1:
        if events[event]["avg"] * length < 100000:
            dates = [date for date in dates if date != "avg"]
            dates_to_process = dates
            while len(dates_to_process) > length:
                from_date = dates_to_process[0]
                to_date = dates_to_process[length - 1]
                result.append(RequestInput(from_day=from_date,
                                           to_day=to_date,
                                           event_type=[event]))
                dates_to_process = dates_to_process[length:]
            else:
                from_date = dates_to_process[0]
                to_date = dates_to_process[-1]
                result.append(RequestInput(from_day=from_date,
                                           to_day=to_date,
                                           event_type=[event]))

            break
        else:
            length = int(length / 2)
            recursion(events, length, result, event, dates)

    return result


def get_request_input(matrix: List[Metadata]) -> List[RequestInput]:
    events = {}

    all_dates = []
    all_event_types = []
    result = []

    # defining dictionary
    for event in matrix:
        if event.day not in all_dates:
            all_dates.append(event.day)
        if event.event_type not in all_event_types:
            all_event_types.append(event.event_type)

    for event in matrix:
        if event.event_type not in events:
            events[event.event_type] = {}
        events[event.event_type][event.day] = event.count

    # adding average count to dict
    for event, dates in events.items():
        sum_count = 0
        for count in dates.values():
            sum_count += count

        avg = sum_count / len(events[event])
        events[event]["avg"] = avg

    processed_entities = {}
    small_events = {}
    # start processing
    for event, dates in events.items():
        # process all records where count is about 50000 since adding new date of the same event will likely cause
        # exceeding over 100000
        if 40000 < dates["avg"] < 60000:

            for date, count in dates.items():
                if date != "avg":
                    result.append(RequestInput(from_day=date,
                                               to_day=date,
                                               event_type=[event]))
                    if event not in processed_entities:
                        processed_entities[event] = []
                    processed_entities[event].append(date)
        # process all records where count is over 100000 for each records
        for date, count in dates.items():
            if count > 100000 and date != "avg":
                result.append(RequestInput(from_day=date,
                                           to_day=date,
                                           event_type=[event]))
                if event not in processed_entities:
                    processed_entities[event] = []
                processed_entities[event].append(date)
        sum_event = 0
        # process small counting amount for all range of dates
        if 0 < dates["avg"] <= 1000:
            for date, count in dates.items():
                if date != "avg":
                    sum_event += count
            small_events[event] = sum_event

    # process small events
    result_sum = 0
    result_events = []
    for event, sum_count in small_events.items():
        result_sum += sum_count
        result_events.append(event)
        if result_sum > 100000:
            result.append(RequestInput(from_day='2024-08-01',
                                       to_day='2024-08-31',
                                       event_type=result_events[:-1]))
            result_sum = sum_count
            result_events = [event]
        # delete item in order to not be processed by further steps
        events.pop(event)

    # delete items in order to not be processed by further steps
    for event, dates in processed_entities.items():
        for date in dates:
            events[event].pop(date)

    for event, dates in events.items():
        if dates["avg"] == 0:
            continue
        # process the rest of the records splitting them into bunches if sum of event's count (avg * lenght of bunch)
        # of the bunch exceeds 100000
        result = recursion(events, len(events[event]), result, event, dates)

    return result


if __name__ == '__main__':
    matrix = read_csv('matrix.csv')
    request_input_list = get_request_input(matrix)
    print(json.dumps([dataclasses.asdict(r) for r in request_input_list], indent=2))
