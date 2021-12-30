from typing import List, Dict

import requests


def json_to_list(data, ret_list) -> List:
    """ Convert json object to list"""
    for entry in data.items():
        ret_dict = entry[1]
        ret_dict["id"] = entry[0]
        ret_list.append(ret_dict)
    return ret_list


def execute_transformation(r: requests.Response) -> List[Dict]:
    """
    Run transformation and check whether request response was positive (200)
    """
    ret_list = []
    if request_status_check(r):
        data = r.json()
        ret_list = json_to_list(data, ret_list)
    return ret_list


def request_status_check(r: requests.Response) -> bool:
    """ Check if a request response was positive """
    if r.status_code != 200:
        return False
    else:
        return True
