
def json_to_df(data, ret_list):
    for entry in data.items():
        ret_dict = entry[1]
        ret_dict["id"] = entry[0]
        ret_list.append(ret_dict)
    return ret_list
