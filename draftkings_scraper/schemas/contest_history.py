from marshmallow import Schema, fields, EXCLUDE


class ContestHistorySchema(Schema):
    """Schema for validating contest history data before database insertion."""

    class Meta:
        unknown = EXCLUDE

    # Primary key (composite)
    entry_id = fields.Integer(required=True)
    contest_id = fields.Integer(required=True)

    # Contest details
    sport = fields.String(allow_none=True)
    game_type = fields.String(allow_none=True)
    entry = fields.String(allow_none=True)
    opponent = fields.String(allow_none=True)
    contest_date_est = fields.DateTime(allow_none=True)

    # Results
    lineup_rank = fields.Integer(allow_none=True)
    points = fields.Float(allow_none=True)
    winnings_non_ticket = fields.Float(allow_none=True)
    winnings_ticket = fields.String(allow_none=True)

    # Contest info
    contest_entries = fields.Integer(allow_none=True)
    entry_fee = fields.Float(allow_none=True)
    prize_pool = fields.Float(allow_none=True)
    places_paid = fields.Integer(allow_none=True)
