from marshmallow import Schema, fields, EXCLUDE


class DraftGroupSchema(Schema):

    class Meta:
        unknown = EXCLUDE

    # Primary key
    draft_group_id = fields.Integer(required=True)

    # Configuration
    allow_ugc = fields.Boolean(allow_none=True)
    contest_start_time_suffix = fields.String(allow_none=True)
    contest_start_time_type = fields.Integer(allow_none=True)
    contest_type_id = fields.Integer(allow_none=True)
    draft_group_series_id = fields.Integer(allow_none=True)
    draft_group_tag = fields.String(allow_none=True)

    # Game info
    game_count = fields.Integer(allow_none=True)
    game_set_key = fields.String(allow_none=True)
    game_type = fields.String(allow_none=True)
    game_type_id = fields.Integer(allow_none=True)
    games = fields.String(allow_none=True)

    # Sorting
    sort_order = fields.Integer(allow_none=True)
    sport = fields.String(allow_none=True)
    sport_sort_order = fields.Integer(allow_none=True)

    # Dates
    start_date = fields.String(allow_none=True)
    start_date_est = fields.String(allow_none=True)

    # Status
    is_etl = fields.Boolean(allow_none=True)
