class SentinelAPIError(Exception):
    """Invalid response from DataHub. Base class for more specific exceptions.
    Attributes
    ----------
    msg: str
        The error message.
    response: requests.Response
        The response from the server as a `requests.Response` object.
    """

    def __init__(self, msg="", response=None):
        self.msg = msg
        self.response = response

    def __str__(self):
        if self.response is None:
            return self.msg
        if self.response.reason:
            reason = " " + self.response.reason
        else:
            reason = ""
        return "HTTP status {}{}: {}".format(
            self.response.status_code,
            reason,
            ("\n" if "\n" in self.msg else "") + self.msg,
        )


class ReaderException(Exception):
    def __init__(self, method, args):
        self.method = method
        self.args = args

    def __str__(self):
        return "Reader error calling {} with {}".format(
            self.method,  self.args
        )


class SearchError(Exception):
    def __init__(self, exception):
        self.exception = exception

    def __str__(self):
        return "Search error: {}".format(
            str(self.rexception),
        )
