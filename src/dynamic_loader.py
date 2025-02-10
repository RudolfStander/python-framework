from importlib import util as importlib_util
from os import walk
from pathlib import Path
from types import ModuleType
from typing import List


def load_submodules(parent_module: ModuleType, required_attributes: List[str] = None):
    """
    Load all submodules given a parent module.

    If a list of required attributes is presented, the loaded modules will be scanned
    for the attributes and filtered out if all attributes are not present.
    """

    submodules = load_submodule_details(parent_module)

    loaded_submodules = []

    for submodule_details in submodules:
        spec = importlib_util.spec_from_file_location(submodule_details[0], submodule_details[1])
        submodule = spec.loader.load_module()

        attributes_found = True

        if required_attributes is not None and len(required_attributes) > 0:
            for attrib in required_attributes:
                if attrib not in dir(submodule):
                    attributes_found = False
                    break

        if attributes_found:
            spec.loader.exec_module(submodule)
            loaded_submodules.append(submodule)

    return loaded_submodules


def load_submodule_details(parent_module: ModuleType):
    package_path = Path(parent_module.__file__).parent

    submodules = []

    for (dirpath, dirnames, filenames) in walk(package_path):
        if "__pycache__" in dirpath:
            continue

        child_path = (
            ""
            if str(dirpath).strip() == str(package_path).strip()
            else dirpath.strip().replace(f"{package_path}/", "")
        )
        submodule_package = ""

        if len(child_path) > 0:
            submodule_package = child_path.replace("/", ".")
            submodule_package = f"{parent_module.__name__}.{submodule_package}"

        for filename in filenames:
            if not filename.endswith(".py"):
                continue

            if filename == "__init__.py":
                continue

            submodule = filename.replace(".py", "")
            submodule_details = (
                f"{submodule_package}.{submodule}",
                f"{dirpath}/{filename}",
                submodule_package,
                filename,
            )

            submodules.append(submodule_details)

    return submodules
