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
    json_file_path="./MPlusTracker.json", excel_output_path="MPlusTracker.xlsx"
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
        char_runs = []
        for run in runs:
            if any((member["name"]) == char_name for member in run["party"]):
                tank = next(
                    (
                        f"{member['name']} ({member.get('spec', member.get('class', 'Unknown'))})"
                        for member in run["party"]
                        if member["role"] == "TANK"
                    ),
                    "",
                )
                healer = next(
                    (
                        f"{member['name']} ({member.get('spec', member.get('class', 'Unknown'))})"
                        for member in run["party"]
                        if member["role"] == "HEALER"
                    ),
                    "",
                )
                damagers = [
                    f"{member['name']} ({member.get('spec', member.get('class', 'Unknown'))})"
                    for member in run["party"]
                    if member["role"] == "DAMAGER"
                ]

                char_runs.append(
                    {
                        "Start Time": run["startTime"],
                        "Level": run["level"],
                        "Map Name": run["mapName"],
                        "Tank": tank,
                        "Healer": healer,
                        "DPS1": damagers[0],
                        "DPS2": damagers[1],
                        "DPS3": damagers[2],
                        "Completion Time": run["completionTime"],
                    }
                )

        if char_runs:
            df = pl.DataFrame(char_runs)
            cols = df.columns
            if "Start Time" in cols:
                ordered_cols = (
                    ["Start Time"]
                    + [
                        col
                        for col in cols
                        if col not in ["Start Time", "Completion Time"]
                    ]
                    + ["Completion Time"]
                )
                df = df.select(ordered_cols)
            sheets[char_name[:31]] = df

    workbook = xlsxwriter.Workbook(excel_output_path)
    class_color_map = get_class_color_map()

    for sheet_name, df in sheets.items():
        worksheet = workbook.add_worksheet(sheet_name)

        # Write headers
        header_format = workbook.add_format(
            {"bold": True, "bg_color": "#d9d9d9", "font_size": 16}
        )
        for col, header in enumerate(df.columns):
            worksheet.write(0, col, header, header_format)
            worksheet.set_column(col, col, max(len(header), 15))

        # Write data rows
        for row_num, row in enumerate(df.to_numpy(), start=1):
            for col_num, cell in enumerate(row):
                if isinstance(cell, str) and "(" in cell and ")" in cell:
                    class_name = None
                    for member in runs:
                        for party_member in member.get("party", []):
                            if f"{party_member['name']} (" in cell:
                                class_name = party_member.get("class")
                                break
                        if class_name:
                            break
                    cell_format = workbook.add_format(
                        {"bg_color": class_color_map.get(class_name, "#ffffff")}
                    )
                else:
                    cell_format = None
                worksheet.write(row_num, col_num, cell, cell_format)
    workbook.close()


create_excel()
