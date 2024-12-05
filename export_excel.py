import json

import polars as pl
import xlsxwriter


def get_class_color_map():
    return {
        "DEATHKNIGHT": "#C41E3A",
        "DEMONHUNTER": "#A330C9",
        "DRUID": "#FF7C0A",
        "EVOKER": "#33937F",
        "HUNTER": "#AAD372",
        "MAGE": "#3FC7EB",
        "MONK": "#00FF98",
        "PALADIN": "#F48CBA",
        "PRIEST": "#FFFFFF",
        "ROGUE": "#FFF468",
        "SHAMAN": "#0070DD",
        "WARLOCK": "#8788EE",
        "WARRIOR": "#C69B6D",
    }


def create_excel(
    json_file_path="MPlusTracker.json", excel_output_path="MPlusTracker.xlsx"
):
    MY_CHARS = ["Drwn", "Podcast", "Samahan"]
    # Extract Data
    with open(json_file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Extract runs
    runs = data.get("runs")

    player_characters = set(
        char_name
        for char_name in MY_CHARS
        if any(
            char_name == party_member["name"]
            for run in runs
            for party_member in run.get("party", [])
        )
    )

    sheets = {}

    for char_name in player_characters:
        char_runs = [
            {
                "Start Time": run["startTime"],
                "Level": run["level"],
                "Map Name": run["mapName"],
                **{
                    f"Party {i+1}": f"{member['name']} ({member['class']})"
                    for i, member in enumerate(run["party"])
                },
                "Completion Time": run["completionTime"],
            }
            for run in runs
            if any((member["name"]) == char_name for member in run["party"])
        ]

        if char_runs:
            df = pl.DataFrame(char_runs)
            cols = df.columns
            if "Start Time" in cols:
                ordered_cols = ["Start Time"] + [
                    col for col in cols if col != "Start Time"
                ]
                df = df.select(ordered_cols)
            sheets[char_name[:31]] = df

    workbook = xlsxwriter.Workbook(excel_output_path)
    class_color_map = get_class_color_map()

    for sheet_name, df in sheets.items():
        worksheet = workbook.add_worksheet(sheet_name)

        # Write headers
        header_format = workbook.add_format({"bold": True, "bg_color": "#d9d9d9"})
        for col, header in enumerate(df.columns):
            worksheet.write(0, col, header, header_format)

        # Write data rows
        for row_num, row in enumerate(df.to_numpy(), start=1):
            for col_num, cell in enumerate(row):
                if isinstance(cell, str) and "(" in cell and ")" in cell:
                    class_name = cell.split("(")[-1].strip(")")
                    cell_format = workbook.add_format(
                        {"bg_color": class_color_map.get(class_name, "#ffffff")}
                    )
                else:
                    cell_format = None
                worksheet.write(row_num, col_num, cell, cell_format)
    workbook.close()


create_excel()
