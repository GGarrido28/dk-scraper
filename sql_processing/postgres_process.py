import os

from mg.db.postgres_manager import PostgresManager
from mg.logging.logger_manager import LoggerManager

from queries import get_query


class DBManager:
    def __init__(self, sport, query_type="date", query_date=None):
        self.sport = sport
        self.process_name = f"draftkings_etl"
        self.script_name = os.path.basename(__file__)
        self.script_path = os.path.dirname(__file__)
        self.logger = LoggerManager(
            self.script_name,
            self.script_path,
            self.process_name,
            sport,
            sport,
            "core",
        )
        self.logger.log_exceptions()
        self.database = "defaultdb"
        self.schema = "draftkings"
        self.draftkings_connection = PostgresManager(
            "digital_ocean", self.database, self.schema, return_logging=True
        )
        self.control_db = "defaultdb"
        self.control_schema = "control"
        self.control_connection = PostgresManager(
            "digital_ocean", self.control_db, self.control_schema, return_logging=True
        )
        self.step_id = 0
        self.draftkings_tables_contest_id = ["contest", "payout", "player_results"]
        self.draftkings_tables_draft_group_id = ["draft_groups", "player_salary"]
        self.query = get_query(
            sport=sport, query_type=query_type, query_date=query_date
        )

    def _etl_records(self, table, records, sport_connection, id):
        if not records:
            msg = f"No records to insert into {table} for {id}"
            self.logger.log(level="info", message=msg)
            return
        if isinstance(records, dict):
            records = [records]
        msg = f"Inserting {len(records)} records into {table} for {id}"
        self.logger.log(level="info", message=msg)
        try:
            sport_connection.insert_rows(
                table, records[0].keys(), records, contains_dicts=True, update=True
            )
        except Exception as e:
            msg = f"Failed to insert records into {table}"
            self.logger.log(level="error", message=msg)

    def _process_draftkings_table(self, draft_group_id, sport_connection):
        for table in self.draftkings_tables_draft_group_id:
            q = f"""
                    SELECT *
                    FROM {table}
                    WHERE 
                        draft_group_id in ({draft_group_id})
                """
            records = self.draftkings_connection.execute(q)
            self._etl_records(table, records, sport_connection, draft_group_id)

        q = f"""
                SELECT *
                FROM contests
                WHERE draft_group_id in ({draft_group_id})
            """
        contests = self.draftkings_connection.execute(q)
        msg = f"Processing {draft_group_id} contest_ids"
        self.logger.log(level="info", message=msg)
        self._etl_records("contests", contests, sport_connection, draft_group_id)
        contest_ids = [contest_id.get("contest_id") for contest_id in contests]
        for contest_id in contest_ids:
            for table in self.draftkings_tables_contest_id:
                msg = f"Processing {contest_id} {table}"
                self.logger.log(level="info", message=msg)
                q = f"""
                        SELECT *
                        FROM {table}
                        WHERE 
                            contest_id in ({contest_id})
                    """
                records = self.draftkings_connection.execute(q)
                self._etl_records(table, records, sport_connection, contest_id)
            q = f"""
                    UPDATE contests
                    SET is_etl = True
                    WHERE contest_id in ({contest_id})
                """
            self.draftkings_connection.execute(q)
            msg = f"Processed {contest_id}"
            self.logger.log(level="info", message=msg)
        return True

    def _process_game_types(self, sport_connection):
        q = """SELECT * FROM game_types"""
        game_types = self.draftkings_connection.execute(q)
        msg = f"Processing game types"
        self.logger.log(level="info", message=msg)
        self._etl_records("game_types", game_types, sport_connection, "game_types")

    def _process_sport(self, sport):
        if sport not in ["NFL", "CFB", "NHL", "GOLF", "MMA", "MLB"]:
            msg = f"Sport {sport} not supported"
            self.logger.log(level="error", message=msg)
            return
        sport_connection = PostgresManager(
            "digital_ocean", sport.lower(), "draftkings", return_logging=False
        )
        draft_group_ids = self.draftkings_connection.execute(self.query)
        draft_group_ids = [
            draft_group_id.get("draft_group_id") for draft_group_id in draft_group_ids
        ]
        for draft_group_id in draft_group_ids:
            msg = f"Processing {sport} {draft_group_id}"
            self.logger.log(level="info", message=msg)
            complete = self._process_draftkings_table(draft_group_id, sport_connection)
            if complete:
                q = f"""
                        UPDATE draft_groups
                        SET is_etl = True
                        WHERE draft_group_id in ({draft_group_id})
                    """
                self.draftkings_connection.execute(q)
                msg = f"Processed {sport} {draft_group_id}"
                self.logger.log(level="info", message=msg)
            else:
                msg = f"Failed to process {sport} {draft_group_id}"
                self.logger.log(level="error", message=msg)
        self._process_game_types(sport_connection)
        sport_connection.close()

    def _process_draftkings_tables(self, sport=None):
        q = "SELECT sport FROM contest_scrape"
        sports = self.draftkings_connection.execute(q)
        sports = [sport.get("sport") for sport in sports]
        if sport:
            sports = [sport]
        for sport in sports:
            msg = f"Processing {sport}"
            self.logger.log(level="info", message=msg)
            self._process_sport(sport)

    def _close_connections(self):
        self.draftkings_connection.close()
        self.control_connection.close()

    def process(self):
        try:
            self._process_draftkings_tables(self.sport)
        except Exception as e:
            msg = "Failed to process draftkings tables"
            self.logger.log(level="error", message=msg)
            raise e
        finally:
            self.logger.close_logger()
            self._close_connections()


if __name__ == "__main__":
    # dbm = DBManager("draftkings_etl", sport="MLB", query_type="date", query_date="2024-06-19")
    import datetime

    # start_date = '2024-06-01'
    # end_date = '2024-06-22'
    # for i in range((datetime.datetime.strptime(end_date, "%Y-%m-%d") - datetime.datetime.strptime(start_date, "%Y-%m-%d")).days + 1):
    #     dbm = DBManager("draftkings_etl", sport="MLB", query_type="date", query_date=(datetime.datetime.strptime(start_date, "%Y-%m-%d") + datetime.timedelta(days=i)).strftime("%Y-%m-%d"))
    #     ## TODO FIX SCHEMA IN NFL and CFB schemas for contest table
    #     dbm.process()
    sports = ["NFL", "CFB", "NHL", "GOLF", "MMA", "MLB"]
    sports = ["MMA", "NHL", "GOLF", "MLB"]
    for sport in sports:
        dbm = DBManager(sport=sport, query_type="all")
        dbm.process()
