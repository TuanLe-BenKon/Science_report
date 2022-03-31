from marshmallow import Schema, fields


class CustomerEmailSchema(Schema):
    id = fields.Integer(required=False)
    user_id = fields.String(required=True)
    user_name = fields.String(required=True)
    external = fields.String(required=False)
    internal = fields.String(required=False)
