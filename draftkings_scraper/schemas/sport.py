from marshmallow import Schema, fields, EXCLUDE


class SportSchema(Schema):

    class Meta:
        unknown = EXCLUDE

    # Primary key
    sport_id = fields.Integer(required=True)

    # Sport details
    full_name = fields.String(allow_none=True)
    sort_order = fields.Integer(allow_none=True)
    has_public_contests = fields.Boolean(allow_none=True)
    is_enabled = fields.Boolean(allow_none=True)
    region_full_sport_name = fields.String(allow_none=True)
    region_abbreviated_sport_name = fields.String(allow_none=True)
