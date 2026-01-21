from mg.db.postgres_manager import PostgresManager

import logging

logging.basicConfig(level=logging.INFO)


class DBManager:
    def __init__(
        self, sport, query_start_date=None, query_end_date=None, test_mode=True
    ):
        self.sport = sport
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
        self.query_start_date = query_start_date
        self.query_end_date = query_end_date
        self.test_mode = test_mode
        if self.test_mode:
            logging.info(f"Test mode enabled")

    def _get_contest_ids(self, sport, start_date, end_date):
        q = f"""
            select distinct contest_id
            from contests c 
            left join draft_groups dg on c.draft_group_id =dg.draft_group_id 
            where 
                dg.sport='{sport}' AND 
                cast(c.start_time as date) BETWEEN '{start_date}' AND '{end_date}'
        """
        self.contest_ids = self.draftkings_connection.execute(q)
        self.contest_ids = [str(x["contest_id"]) for x in self.contest_ids]
        self.len_contest_ids = len(self.contest_ids)
        logging.info(f"Found {self.len_contest_ids} contest_ids")

    def _get_draft_groups(self, sport, start_date, end_date):
        q = f"""
            select distinct dg.draft_group_id
            from contests c 
            left join draft_groups dg on c.draft_group_id = dg.draft_group_id 
            where 
                dg.sport='{sport}' AND 
                cast(c.start_time as date) BETWEEN '{start_date}' AND '{end_date}'
        """
        self.draft_group_ids = self.draftkings_connection.execute(q)
        self.draft_group_ids = [str(x["draft_group_id"]) for x in self.draft_group_ids]
        self.len_draft_group_ids = len(self.draft_group_ids)
        logging.info(f"Found {self.len_draft_group_ids} draft_group_ids")

    def _cleanup_tables(self):
        player_salary_count = 0
        player_results_count = 0
        payout_structure_count = 0
        payout_count = 0
        game_sets_count = 0
        draft_groups_count = self.len_draft_group_ids
        contests_count = 0
        contest_count = self.len_contest_ids

        contest_id_tables = {
            "contest": contest_count,
            "contests": contests_count,
            "payout": payout_structure_count,
            "payout_structure": payout_count,
            "player_results": player_results_count,
        }

        draft_group_id_tables = {
            "draft_groups": draft_groups_count,
            "player_salary": player_salary_count,
        }

        game_sets_table = {"game_sets": game_sets_count}
        for table, value in contest_id_tables.items():
            logging.info(f"Cleaning up {table} table")
            count_query = f"""
                select count(*) as count from {table} where contest_id in {tuple(self.contest_ids)}
            """
            count = self.draftkings_connection.execute(count_query)
            count = count[0]["count"]
            logging.info(f"Found {count} records in {table} table")
            contest_id_tables[table] = count
            q = f"""
                DELETE FROM {table}
                WHERE contest_id in {tuple(self.contest_ids)}
            """
            if self.test_mode:
                logging.info(f"Test mode enabled, not deleting records")
            else:
                self.draftkings_connection.execute(q)
                logging.info(f"Deleted {count} records from {table} table")

        for table, value in draft_group_id_tables.items():
            logging.info(f"Cleaning up {table} table")
            count_query = f"""
                select count(*) as count from {table} where draft_group_id in {tuple(self.draft_group_ids)}
            """
            count = self.draftkings_connection.execute(count_query)
            count = count[0]["count"]
            logging.info(f"Found {count} records in {table} table")
            draft_group_id_tables[table] = count
            q = f"""
                DELETE FROM {table}
                WHERE draft_group_id in {tuple(self.draft_group_ids)}
            """
            if self.test_mode:
                logging.info(f"Test mode enabled, not deleting records")
            else:
                self.draftkings_connection.execute(q)
                logging.info(f"Deleted {count} records from {table} table")

        for table, value in game_sets_table.items():
            logging.info(f"Cleaning up {table} table")
            count_query = f"""
                select count(*) as count from {table} where operator_sid in {tuple(self.draft_group_ids)}
            """
            count = self.draftkings_connection.execute(count_query)
            count = count[0]["count"]
            logging.info(f"Found {count} records in {table} table")
            game_sets_table[table] = count
            q = f"""
                DELETE FROM {table}
                WHERE operator_sid in {tuple(self.draft_group_ids)}
            """
            if self.test_mode:
                logging.info(f"Test mode enabled, not deleting records")
            else:
                self.draftkings_connection.execute(q)
                logging.info(f"Deleted {count} records from {table} table")

        logging.info(f"Finished cleaning up tables")
        logging.info(f"Deleted {contest_count} records from contest_id tables")
        logging.info(f"Deleted {draft_groups_count} records from draft_group_id tables")
        return contest_id_tables, draft_group_id_tables, game_sets_table

    def store_cleanup(self, contest_id_tables, draft_group_id_tables, game_sets_table):
        update_record = []
        for contest_id in contest_id_tables:
            record = {
                "table_name": contest_id,
                "record_count": contest_id_tables[contest_id],
                "message": f"Deleted records from contest_id tables for {self.sport} for contests between {self.query_start_date} - {self.query_end_date}",
            }
            update_record.append(record)
        for draft_group_id in draft_group_id_tables:
            record = {
                "table_name": draft_group_id,
                "record_count": draft_group_id_tables[draft_group_id],
                "message": f"Deleted records from draft_group_id tables for {self.sport} for contests between {self.query_start_date} - {self.query_end_date}",
            }
            update_record.append(record)
        for game_set in game_sets_table:
            record = {
                "table_name": game_set,
                "record_count": game_sets_table[game_set],
                "message": f"Deleted records from game_sets tables for {self.sport} for contests between {self.query_start_date} - {self.query_end_date}",
            }
            update_record.append(record)

        self.control_connection.insert_rows(
            "database_cleanup",
            update_record[0].keys(),
            update_record,
            contains_dicts=True,
            update=True,
        )
        logging.info(f"Stored cleanup results in database_cleanup table")

    def cleanup(self):
        logging.info(
            f"Starting cleanup process for {self.sport} for contests starting before {self.query_end_date}"
        )
        self._get_contest_ids(self.sport, self.query_start_date, self.query_end_date)
        self._get_draft_groups(self.sport, self.query_start_date, self.query_end_date)
        contest_id_tables, draft_group_id_tables, game_sets_table = (
            self._cleanup_tables()
        )
        self.store_cleanup(contest_id_tables, draft_group_id_tables, game_sets_table)
        self.draftkings_connection.close()
        self.control_connection.close()
        logging.info(f"Closed connections")


if __name__ == "__main__":
    db = DBManager(
        "NFL",
        query_start_date="2024-08-01",
        query_end_date="2024-08-30",
        test_mode=True,
    )
    db.cleanup()
