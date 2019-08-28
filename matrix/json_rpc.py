"""
Methods to implement jsonrpc over asyncio.
"""
# pylint: disable=broad-except
# pylint: disable=too-many-return-statements

import json
from uuid import uuid4

import logbook

log = logbook.Logger(__name__)


def rpc_parse(line):
    """
    Parse a jsonrpc request and check correctness.
    """

    try:
        request = json.loads(line)
        if not isinstance(request, dict):
            return None, "Request object is not of type object"
    except ValueError:
        if __debug__:
            log.debug(f"Failed to parse RPC request\n{line}")
        return None, "Failed to parse RPC request"

    try:
        if request["jsonrpc"] != "2.0":
            return "Incompatible RPC version: jsonrpc != '2.0'"
    except KeyError:
        return request, "JsonRPC version missing in request"

    try:
        if not isinstance(request["method"], str):
            return request, "Method name is not a string"
    except KeyError:
        return request, "Method name missing in request"

    if "params" in request:
        if not isinstance(request["params"], (list, dict)):
            return request, "Parameters can only be of type object or array"

    return request, None


def rpc_error(message, request=None):
    """
    Generate the rpc error message.
    """

    response = {"jsonrpc": "2.0", "error": str(message)}

    if request is not None and "id" in request:
        response["id"] = request["id"]

    return response


def rpc_request(method, id=None, **params):  # pylint: disable=redefined-builtin
    """
    Generate the rpc request message.
    """

    request = {"jsonrpc": "2.0", "method": method, "params": params}

    if id is None:
        request["id"] = str(uuid4())
    elif id is False:
        pass
    else:
        request["id"] = str(id)

    return request


def rpc_response(result, request):
    """
    Generate the response message.
    """

    if "id" not in request:
        return None

    response = {"jsonrpc": "2.0", "result": result, "id": request["id"]}

    return response


async def rpc_dispatch(method_map, line):
    """
    Dispatch the proper method.
    """

    request, error = rpc_parse(line)
    if error is not None:
        return rpc_error(error, request)

    method = request["method"]
    if method not in method_map:
        return rpc_error("Unknown RPC method", request)

    if "params" in request:
        params = request["params"]
        if isinstance(params, list):
            args, kwargs = params, {}
        else:  # isinstance(params, dict)
            args, kwargs = [], params
    else:
        args, kwargs = [], {}

    try:
        response = await method_map[method](*args, **kwargs)
    except Exception as e:
        log.exception(f"Error dispatching {method}")
        return rpc_error(e, request)

    return rpc_response(response, request)
