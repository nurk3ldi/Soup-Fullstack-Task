"""One error shape for the whole API: {"error": "<code>"}. Bodies never carry IDs, SQL or traces."""
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException


class ApiError(Exception):
    def __init__(self, status: int, code: str, headers: dict | None = None):
        self.status = status
        self.code = code
        self.headers = headers


def unauthorized() -> ApiError:
    return ApiError(401, "unauthorized")


def not_found() -> ApiError:
    return ApiError(404, "not_found")


_HTTP_CODES = {404: "not_found", 405: "method_not_allowed", 413: "payload_too_large"}


def _response(status: int, code: str, headers: dict | None = None) -> JSONResponse:
    if status == 401:
        headers = {"WWW-Authenticate": "Bearer"}
    return JSONResponse({"error": code}, status_code=status, headers=headers)


def install(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def _api_error(request: Request, exc: ApiError):
        return _response(exc.status, exc.code, exc.headers)

    @app.exception_handler(RequestValidationError)
    async def _validation(request: Request, exc: RequestValidationError):
        # FastAPI's default 422 echoes the offending input back; ours does not.
        return _response(422, "invalid_request")

    @app.exception_handler(StarletteHTTPException)
    async def _http(request: Request, exc: StarletteHTTPException):
        return _response(exc.status_code, _HTTP_CODES.get(exc.status_code, "error"))
