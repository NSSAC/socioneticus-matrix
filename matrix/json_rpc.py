"""
Methods to implement jsonrpc over asyncio.
"""
# pylint: disable=broad-except

import json

def rpc_parse(line):
    """
    Parse a jsonrpc request and check correctness.
    """

    try:
        request = json.loads(line)
        assert isinstance(request, dict)
    except (ValueError, AssertionError):
        return None, "Malformatted RPC request"

    try:
        assert request["jsonrpc"] == "2.0"
    except (KeyError, AssertionError):
        return request, "Incompatible RPC version: jsonrpc != 2.0"

    try:
        assert isinstance(request["method"], str)
    except (KeyError, AssertionError):
        return request, "Method name is not a string"

    if "params" in request:
        if not isinstance(request["params"], (list, dict)):
            return request, "Parameters can only be of type object or array"

    return request, None

def rpc_error(message, request=None):
    """
    Generate the rpc error message.
    """

    response = {
        "jsonrpc": "2.0",
        "error": str(message)
    }

    if request is not None and "id" in request:
        response["id"] = request["id"]

    return json.dumps(response)

def rpc_response(result, request):
    """
    Generate the response message.
    """

    if "id" not in request:
        return None

    response = {
        "jsonrpc": "2.0",
        "result": result,
        "id": request["id"]
    }

    return json.dumps(response)

async def rpc_dispatch(method_map, async_method_map, line):
    """
    Dispatch the proper method.
    """

    request, error = rpc_parse(line)
    if error is not None:
        return rpc_error(error, request)

    method = request["method"]
    if method not in method_map and method not in async_method_map:
        return rpc_error("Unknown RPC method", request)

    if "params" in request:
        params = request["params"]
        if isinstance(params, list):
            args, kwargs = params, {}
        else: # isinstance(params, dict)
            args, kwargs = [], params
    else:
        args, kwargs = [], {}

    if method in method_map:
        try:
            response = method_map[method](*args, **kwargs)
        except Exception as e:
            return rpc_error(e, request)
    else: # method in async_method_map:
        try:
            response = await async_method_map[method](*args, **kwargs)
        except Exception as e:
            return rpc_error(e, request)

    return rpc_response(response, request)
