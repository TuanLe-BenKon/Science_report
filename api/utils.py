from flask import Response, jsonify


def message_resp(text: str = "succeeded", status_code: int = 200) -> Response:
    return jsonify(msg=text), status_code
