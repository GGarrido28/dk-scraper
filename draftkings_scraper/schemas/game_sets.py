from marshmallow import Schema, fields, EXCLUDE


class CompetitionSchema(Schema):
    """Schema for competition/game data within a game set."""

    class Meta:
        unknown = EXCLUDE

    # Primary key
    game_id = fields.Integer(required=True, data_key="GameId")

    # Team IDs
    away_team_id = fields.Integer(allow_none=True, data_key="AwayTeamId")
    home_team_id = fields.Integer(allow_none=True, data_key="HomeTeamId")

    # Scores
    home_team_score = fields.Integer(allow_none=True, data_key="HomeTeamScore")
    away_team_score = fields.Integer(allow_none=True, data_key="AwayTeamScore")

    # Team info
    home_team_city = fields.String(allow_none=True, data_key="HomeTeamCity")
    away_team_city = fields.String(allow_none=True, data_key="AwayTeamCity")
    home_team_name = fields.String(allow_none=True, data_key="HomeTeamName")
    away_team_name = fields.String(allow_none=True, data_key="AwayTeamName")

    # Game info
    start_date = fields.String(allow_none=True, data_key="StartDate")
    location = fields.String(allow_none=True, data_key="Location")
    sport = fields.String(allow_none=True, data_key="Sport")
    status = fields.String(allow_none=True, data_key="Status")
    description = fields.String(allow_none=True, data_key="Description")
    full_description = fields.String(allow_none=True, data_key="FullDescription")

    # Game state
    last_play = fields.String(allow_none=True, data_key="LastPlay")
    team_with_possession = fields.Integer(allow_none=True, data_key="TeamWithPossession")
    time_remaining_status = fields.String(allow_none=True, data_key="TimeRemainingStatus")

    # Series info
    series_type = fields.Integer(allow_none=True, data_key="SeriesType")
    number_of_games_in_series = fields.Integer(allow_none=True, data_key="NumberOfGamesInSeries")
    series_info = fields.String(allow_none=True, data_key="SeriesInfo")

    # Competition ordinals
    home_team_competition_ordinal = fields.Integer(allow_none=True, data_key="HomeTeamCompetitionOrdinal")
    away_team_competition_ordinal = fields.Integer(allow_none=True, data_key="AwayTeamCompetitionOrdinal")
    home_team_competition_count = fields.Integer(allow_none=True, data_key="HomeTeamCompetitionCount")
    away_team_competition_count = fields.Integer(allow_none=True, data_key="AwayTeamCompetitionCount")

    # Exceptional messages
    exceptional_messages = fields.List(fields.String(), allow_none=True, data_key="ExceptionalMessages")


class GameStyleSchema(Schema):
    """Schema for game style data within a game set."""

    class Meta:
        unknown = EXCLUDE

    # Primary key
    game_style_id = fields.Integer(required=True, data_key="GameStyleId")

    # Configuration
    sport_id = fields.Integer(allow_none=True, data_key="SportId")
    sort_order = fields.Integer(allow_none=True, data_key="SortOrder")
    name = fields.String(allow_none=True, data_key="Name")
    abbreviation = fields.String(allow_none=True, data_key="Abbreviation")
    description = fields.String(allow_none=True, data_key="Description")
    is_enabled = fields.Boolean(allow_none=True, data_key="IsEnabled")
    attributes = fields.Raw(allow_none=True, data_key="Attributes")


class GameSetSchema(Schema):
    """Schema for DraftKings game set data."""

    class Meta:
        unknown = EXCLUDE

    # Primary key
    game_set_key = fields.String(required=True, data_key="GameSetKey")

    # Display info
    contest_start_time_suffix = fields.String(allow_none=True, data_key="ContestStartTimeSuffix")
    tag = fields.String(allow_none=True, data_key="Tag")

    # Nested data
    competitions = fields.List(fields.Nested(CompetitionSchema), allow_none=True, data_key="Competitions")
    game_styles = fields.List(fields.Nested(GameStyleSchema), allow_none=True, data_key="GameStyles")

    # Sorting/timing
    sort_order = fields.Integer(allow_none=True, data_key="SortOrder")
    min_start_time = fields.String(allow_none=True, data_key="MinStartTime")
