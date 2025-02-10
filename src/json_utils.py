from typing import Any, Dict, List, Union


def extract_json_path(
    obj: Dict[str, Any], path: str
) -> Union[None, str, List[Any], Dict[str, Any]]:
    split_path = path.split(".")
    obj_at_path = obj

    for path_entry in split_path:
        if path_entry == "*":
            return obj_at_path

        if path_entry not in obj_at_path:
            return None

        obj_at_path = obj_at_path[path_entry]

    return obj_at_path


def list_to_json(objects: List[Any]):
    return {"items": list(map(lambda x: x.to_json(), objects))}
