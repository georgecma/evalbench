from .generator import QueryGenerator
from databases import DB


class AlloyDBGenerator(QueryGenerator):

    def __init__(self, db, querygenerator_config):
        super().__init__(querygenerator_config)
        self.db = db
        self.name = "alloydb_ai_nl"

    def get_sql(self, prompt: str) -> str:
        processed_nl_config = str(self.db.nl_config).replace("'", "''")
        processed_prompt = prompt.replace("'", "''")
        return (f"SELECT alloydb_ai_nl.get_sql('{processed_nl_config}', "
                f"'{processed_prompt}') ->> 'sql';")

    def generate_internal(self, prompt):
        rows, _, err = self.db.execute(self.get_sql(prompt))
        if err:
            return err
        if not rows:
            return "No SQL query generated."
        sql_query = "\n".join(
            row.get('?column?', '')
            for row in rows
            if '?column?' in row
        )
        return sql_query
