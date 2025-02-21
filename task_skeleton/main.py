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



def get_request_input(matrix: List[Metadata]) -> List[RequestInput]:
  
    return []


if __name__ == '__main__':
    matrix = read_csv('matrix.csv')
    request_input_list = get_request_input(matrix)
    print(json.dumps([dataclasses.asdict(r) for r in request_input_list], indent=2))
