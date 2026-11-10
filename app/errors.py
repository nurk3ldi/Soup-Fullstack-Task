"""One error shape for the whole API: {"error": "<code>"}. Bodies never carry IDs, SQL or traces."""
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException


class ApiError(Exception):
    def __init__(self, status: int, code: str):
        self.status = status
        self.code = code


def unauthorized() -> ApiError:
    return ApiError(401, "unauthorized")


def not_found() -> ApiError:
    return ApiError(404, "not_found")


_HTTP_CODES = {404: "not_found", 405: "method_not_allowed"}


def _response(status: int, code: str) -> JSONResponse:
    headers = {"WWW-Authenticate": "Bearer"} if status == 401 else None
    return JSONResponse({"error": code}, status_code=status, headers=headers)


def install(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def _api_error(request: Request, exc: ApiError):
        return _response(exc.status, exc.code)

    @app.exception_handler(RequestValidationError)
    async def _validation(request: Request, exc: RequestValidationError):
        # FastAPI's default 422 echoes the offending input back; ours does not.
        return _response(422, "invalid_request")

    @app.exception_handler(StarletteHTTPException)
    async def _http(request: Request, exc: StarletteHTTPException):
        return _response(exc.status_code, _HTTP_CODES.get(exc.status_code, "error"))
