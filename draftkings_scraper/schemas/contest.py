from marshmallow import Schema, fields, EXCLUDE


class ContestEntrySchema(Schema):

    class Meta:
        unknown = EXCLUDE

    # Primary key (composite)
    contest_id = fields.Integer(required=True)
    entry_id = fields.Integer(required=True)

    # Entry details
    lineup_rank = fields.Integer(allow_none=True)
    entry_name = fields.String(allow_none=True)
    entry = fields.Integer(allow_none=True)
    total_entries = fields.Integer(allow_none=True)
    lineup = fields.String(allow_none=True)
    points = fields.Float(allow_none=True)
