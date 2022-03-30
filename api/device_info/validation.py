from marshmallow import Schema, fields


class DeviceInfoSchema(Schema):
    id = fields.Integer(required=False)
    user_id = fields.String(required=True)
    user_name = fields.String(required=True)
    device_id = fields.String(required=False)
    device_name = fields.String(required=False)
    status = fields.Integer(required=False)
    outdoor_unit = fields.Integer(required=False)
