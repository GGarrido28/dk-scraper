from marshmallow import Schema, fields, pre_load, EXCLUDE
import json


class ContestSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    # Primary key
    contest_id = fields.Integer(required=True)

    # Contest details
    contest_name = fields.String(allow_none=True)
    entry_fee = fields.Float(allow_none=True)
    crown_amount = fields.Integer(allow_none=True)
    max_entries = fields.Integer(allow_none=True)
    entries_per_user = fields.Integer(allow_none=True)
    draft_group_id = fields.Integer(allow_none=True)

    # JSON fields (stored as strings)
    pd = fields.String(allow_none=True)
    attr = fields.String(allow_none=True)

    # Payout
    po = fields.Float(allow_none=True)

    # Boolean attributes
    guranteed = fields.Boolean(allow_none=True)
    starred = fields.Boolean(allow_none=True)
    double_up = fields.Boolean(allow_none=True)
    fifty_fifty = fields.Boolean(allow_none=True)
    league = fields.Boolean(allow_none=True)
    multiplier = fields.Boolean(allow_none=True)
    qualifier = fields.Boolean(allow_none=True)

    # Status flags
    is_final = fields.Boolean(allow_none=True)
    is_cancelled = fields.Boolean(allow_none=True)
    is_downloaded = fields.Boolean(allow_none=True)
    is_empty = fields.Boolean(allow_none=True)
    is_etl = fields.Boolean(allow_none=True)
    is_ignored = fields.Boolean(allow_none=True)

    # URL and dates
    contest_url = fields.String(allow_none=True)
    start_time = fields.DateTime(allow_none=True)
    contest_date = fields.String(allow_none=True)

    @pre_load
    def serialize_json_fields(self, data, **kwargs):
        """Ensure JSON fields are serialized as strings."""
        if "pd" in data and data["pd"] is not None:
            if not isinstance(data["pd"], str):
                data["pd"] = json.dumps(data["pd"])
        if "attr" in data and data["attr"] is not None:
            if not isinstance(data["attr"], str):
                data["attr"] = json.dumps(data["attr"])
        return data
