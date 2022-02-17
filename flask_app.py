from dotenv import load_dotenv, find_dotenv
from werkzeug.exceptions import HTTPException
from marshmallow import Schema, fields, ValidationError
from flask import Flask, render_template, Response, jsonify

from api.tasks import get_device_data

app = Flask(__name__)
load_dotenv(find_dotenv())


@app.route("/")
def hello():
    return render_template("home.html")


@app.route("/health")
def health():
    return Response(response="success", status=200)


@app.route(
    "/v1/energy-alert/<string:device_id>/<string:user_id>/<int:init_timestamp>/",
    methods=["GET"],
)
def alert(device_id, user_id, init_timestamp):

    return get_device_data(device_id, user_id, init_timestamp)


@app.errorhandler(Exception)
def handle_error(e):
    code = 500
    if isinstance(e, HTTPException):
        code = e.code
    return jsonify(error=str(e)), code


if __name__ == "__main__":
    app.run(debug=True, port=8001)