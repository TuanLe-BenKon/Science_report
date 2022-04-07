from marshmallow import Schema, fields


class SchedulerlSchema(Schema):
    user_id = fields.String(required=True)
    scheduler_name = fields.String(required=False)
