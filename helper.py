from datetime import datetime
import os
import json


today = datetime.now().strftime("%d-%m-%Y")
log_file = f"logs/log-{today}.log"
os.makedirs(os.path.dirname(log_file), exist_ok=True)

DEFAULT_CONFIG = {
    "host": "localhost",
    "database": "easytrade_db",
    "user": "easytrade",
    "password": "masterkey",
    "price_type": 1,
    "check_time": 10,
    "plu_file_path": r"C:\Program Files (x86)\RLS1000\easytrade_plu.txp",
    "handle_big_price": {
        "active": True,
        "divider": 100
    },
    "units": [
        {
            "name": "Весовой",
            "easy_trade_id": 2,
            "scale_unit_id": 4,
            "barcode_type": 7,
            "prefix": 22,
            "label_id": 0
        },
        {
            "name": "Штучный",
            "easy_trade_id": 1,
            "scale_unit_id": 9,
            "barcode_type": 7,
            "prefix": 23,
            "label_id": 0
        },
    ]
}

def get_date():
    now = datetime.now()
    return now.strftime("%d.%m.%Y %H:%M:%S")


def create_query_arg(units_config):
    et_units_id = tuple([value["easy_trade_id"] for value in units_config])
    if len(et_units_id) == 1:
        return f"AND G.gd_unit = {et_units_id[0]}"
    else:
        return f"AND G.gd_unit IN {et_units_id}"


def write_log_file(text):
    with open(log_file, "a", encoding='utf-8') as file:
        formatted_text = f"{get_date()} - {text}\n"
        file.write(formatted_text)
        print(formatted_text)

def configure_settings(data_dict=DEFAULT_CONFIG, filename="config.json"):
    if os.path.exists(filename):
        try:
            with open(filename, 'r', encoding='utf-8') as json_file:
                data_dict = json.load(json_file)
            return data_dict
        except FileNotFoundError:
            write_log_file(f"Error: File '{filename}' not found")

        except json.JSONDecodeError:
            write_log_file(f"Error: File '{filename}' contains invalid JSON")
            os.remove(filename)

        except Exception as e:
            write_log_file(f"Error reading JSON file: {e}")
            os.remove(filename)


    try:
        with open(filename, 'w', encoding='utf-8', errors="replace") as json_file:
            json.dump(data_dict, json_file, indent=4, ensure_ascii=False)
    except Exception as e:
        write_log_file(f"Error writing to JSON file: {e}")
    else:
        return data_dict
