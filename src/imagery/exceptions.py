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
