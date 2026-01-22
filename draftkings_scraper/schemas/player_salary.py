from marshmallow import Schema, fields, EXCLUDE


class PlayerSalarySchema(Schema):

    class Meta:
        unknown = EXCLUDE

    # Primary key (composite)
    draft_group_id = fields.Integer(required=True)
    id = fields.Integer(required=True)

    # Player details
    position = fields.String(allow_none=True)
    name_id = fields.String(allow_none=True)
    name = fields.String(allow_none=True)
    roster_position = fields.String(allow_none=True)

    # Salary info
    salary = fields.Float(allow_none=True)

    # Game info
    game_info = fields.String(allow_none=True)
    team_abbrev = fields.String(allow_none=True)

    # Stats
    avg_points_per_game = fields.Float(allow_none=True)
