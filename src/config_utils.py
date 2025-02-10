from os import environ


def load_environment_variable(
    environment_variable: str, default: str = None, error_on_none: bool = False
):
    if (
        environment_variable in environ
        and environ[environment_variable] is not None
        and environ[environment_variable] != ""
    ):
        return environ[environment_variable]

    if default is None and error_on_none:
        raise Exception("Missing environment variable [%s]" % environment_variable)

    return default
