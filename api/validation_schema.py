from marshmallow import Schema, fields


class EnergyAlertTaskSchema(Schema):
    device_id = fields.String(required=True)
    user_id = fields.Integer(required=True)
    init_timestamp = fields.Integer(required=True)
    url = fields.String(required=False)
    in_seconds = fields.Integer(require=False)


class GenReportSchema(Schema):
    user_id = fields.String(required=True)
    track_day = fields.String(required=False)
