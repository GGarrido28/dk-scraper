from marshmallow import Schema, fields, EXCLUDE


class PlayerResultsSchema(Schema):

    class Meta:
        unknown = EXCLUDE

    # Primary key (composite)
    contest_id = fields.Integer(required=True)
    player = fields.String(required=True)
    roster_position = fields.String(required=True)

    # Stats
    percent_drafted = fields.Float(allow_none=True)
    fpts = fields.Float(allow_none=True)
