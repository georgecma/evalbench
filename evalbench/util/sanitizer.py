def sanitize_sql(sql: str):
    return (
        sql.replace(
            "```sql", ""
        )  # required for gemini_1.0_pro, gemini_2.0_flash, gemini_2.5_pro
        .replace(
            "```", ""
        )  # required for gemini_1.0_pro, gemini_2.0_flash, gemini_2.5_pro
        .replace('sql: "', "")
        .replace("\\n", " ")
        .replace("\\", "")
        .replace("  ", "")
        .replace("`", "")
        .replace("google_sql", "")
        .strip()
    )
