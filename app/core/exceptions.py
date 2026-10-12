class ApplicationError(Exception):
    error_code = "application_error"
    status_code = 500

    def __init__(self, message: str, *, details: dict | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}


class UploadTooLargeError(ApplicationError):
    error_code = "upload_too_large"
    status_code = 413


class EmptyUploadError(ApplicationError):
    error_code = "empty_upload"
    status_code = 400


class UploadStorageError(ApplicationError):
    error_code = "upload_storage_failed"
    status_code = 503


class UnsupportedMediaTypeError(ApplicationError):
    error_code = "unsupported_media_type"
    status_code = 415


class UnsupportedCurrencyError(ApplicationError):
    error_code = "unsupported_currency"
    status_code = 422

    def __init__(self, currency: str) -> None:
        super().__init__(
            f"Currency {currency} is not supported",
            details={"currency": currency},
        )


class OrderAlreadyExistsError(ApplicationError):
    error_code = "order_already_exists"
    status_code = 409

    def __init__(self, order_id: str) -> None:
        super().__init__(
            f"Order {order_id} already exists",
            details={"order_id": order_id},
        )


class OrderNotFoundError(ApplicationError):
    error_code = "order_not_found"
    status_code = 404

    def __init__(self, order_id: str) -> None:
        super().__init__(
            f"Order {order_id} was not found",
            details={"order_id": order_id},
        )


class ImportNotFoundError(ApplicationError):
    error_code = "import_not_found"
    status_code = 404


class ImportNotReadyError(ApplicationError):
    error_code = "import_not_ready"
    status_code = 409


class StorageDirectoryUnavailableError(ApplicationError):
    error_code = "storage_directory_unavailable"
    status_code = 503

    def __init__(self, directory: str, *, details: dict | None = None) -> None:
        msg = f"Storage directory unavailable: {directory}"
        super().__init__(msg, details=details or {"directory": directory})
        self.directory = directory
