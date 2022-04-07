from marshmallow import Schema, fields


class SchedulerlSchema(Schema):
    user_id = fields.String(required=True)
    track_day = fields.String(required=False)
