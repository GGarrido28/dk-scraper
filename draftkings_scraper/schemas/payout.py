from marshmallow import Schema, fields, pre_load, EXCLUDE
import json


class PayoutSchema(Schema):
    """Schema for validating payout data before database insertion."""

    class Meta:
        unknown = EXCLUDE

    # Primary key (composite)
    contest_id = fields.Integer(required=True)
    max_position = fields.Integer(required=True)
    min_position = fields.Integer(required=True)

    # JSON field (stored as string)
    original_tier = fields.String(allow_none=True)

    # Payout details
    payout_one_type = fields.String(allow_none=True)
    payout_one_value = fields.Float(allow_none=True)
    payout_two_type = fields.String(allow_none=True)
    payout_two_value = fields.Float(allow_none=True)

    @pre_load
    def serialize_json_fields(self, data, **kwargs):
        """Ensure JSON fields are serialized as strings."""
        if "original_tier" in data and data["original_tier"] is not None:
            if not isinstance(data["original_tier"], str):
                data["original_tier"] = json.dumps(data["original_tier"])
        return data
