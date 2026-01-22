from marshmallow import Schema, fields, pre_load, EXCLUDE
import json


class GameTypeSchema(Schema):

    class Meta:
        unknown = EXCLUDE

    # Primary key
    game_type_id = fields.Integer(required=True)

    # Details
    name = fields.String(allow_none=True)
    description = fields.String(allow_none=True)
    tag = fields.String(allow_none=True)
    sport_id = fields.Integer(allow_none=True)
    draft_type = fields.String(allow_none=True)

    # JSON field (stored as string)
    game_style = fields.String(allow_none=True)

    @pre_load
    def serialize_json_fields(self, data, **kwargs):
        """Ensure JSON fields are serialized as strings."""
        if "game_style" in data and data["game_style"] is not None:
            if not isinstance(data["game_style"], str):
                data["game_style"] = json.dumps(data["game_style"])
        return data
