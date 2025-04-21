import mysql.connector
from mysql.connector import Error
import time
from helper import create_query_arg, configure_settings, write_log_file, validate_unique_integer_string
import sys

# pyinstaller command: pyinstaller --onefile --name=EasyTradeRongtaRLS1000Int main.py

class UpdateData:
    def __init__(self):
        # settings
        self.config = configure_settings()
        self.host = self.config['host']
        self.database = self.config['database']
        self.user = self.config['user']
        self.password = self.config['password']
        self.price_type = self.config['price_type']
        self.check_time = self.config['check_time']
        self.units = self.config['units']
        self.plu_file_path = self.config['plu_file_path']
        self.use_articul = self.config['use_articul']
        self.use_description_as_hotkey = self.config['use_description_as_hotkey']

        self.mysql_conn = None
        self.last_changes = 0
        self.is_mysql_connected = False
        self.connect_mysql()

        self.units_dict = {unit["easy_trade_id"]: unit for unit in self.units}


    def connect_mysql(self):
        try:
            self.mysql_conn = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
            )
        except Error as e:
            write_log_file(f"Can't connect to the MySQL. {e}")
            self.is_mysql_connected = False
            return False
        else:
            self.is_mysql_connected = True
            return True

    def check_mysql_changes(self):
        try:
            mysql_cursor = self.mysql_conn.cursor()
            mysql_cursor.execute("RESET QUERY CACHE")
            query_check_item = """
            SELECT gd_last_update FROM easytrade_db.dir_goods
            ORDER BY gd_last_update DESC
            LIMIT 1
            """
            mysql_cursor.execute(query_check_item)
            last_changed_item = mysql_cursor.fetchone()

            query_check_price = """
            SELECT prc_last_update FROM easytrade_db.dir_prices
            ORDER BY prc_last_update DESC
            LIMIT 1
            """
            mysql_cursor.execute(query_check_price)
            last_changed_price = mysql_cursor.fetchone()

        except Error as e:
            write_log_file(f"Can't connect to the MySQL. {e}")
            self.is_mysql_connected = False
            return False
        else:
            last_operation = last_changed_price if last_changed_price > last_changed_item else last_changed_item

            timestamp_last_operation = last_operation[0].timestamp()

            if self.last_changes < timestamp_last_operation:
                self.last_changes = timestamp_last_operation
                mysql_cursor.close()
                return True
            else:
                mysql_cursor.close()
                return False

    def fetch_products_data(self):
        units_arg = create_query_arg(self.units)
        query_fetch_items = f"""
            SELECT
                G.gd_id,  
                G.gd_code, 
                G.gd_name, 
                G.gd_unit,  
                P.prc_value,
                G.gd_articul,
                G.gd_description
            FROM easytrade_db.dir_goods G
                LEFT JOIN easytrade_db.dir_prices P ON G.gd_id = P.prc_good and P.prc_type = %s
            WHERE 
                G.gd_deleted_mark = 0 
                AND G.gd_deleted = 0 
                AND P.prc_type = %s
                AND P.prc_value > 0
                {units_arg}
            ORDER BY G.gd_code
        """
        try:
            mysql_cursor = self.mysql_conn.cursor()
            mysql_cursor.execute("RESET QUERY CACHE")
            mysql_cursor.execute(query_fetch_items, (self.price_type, self.price_type))
            items = mysql_cursor.fetchall()

        except Error as e:
            write_log_file(f"Can't connect to the MySQL. {e}")
            self.is_mysql_connected = False
            return False

        else:
            mysql_cursor.close()
            if items: return items

    def save_data_with_tabs(self, data, filename):
        with open(filename, 'w', encoding='windows-1251', errors="replace") as file:
            for row in data:
                # Join all items in the row with tab separator
                line = '\t'.join(map(str, row))
                file.write(line + '\n')

    def create_plu_file(self):
        is_changed = self.check_mysql_changes()
        if not is_changed: return
        product_data = self.fetch_products_data()
        if not product_data: return
        formatted_data = []
        articul_list = []
        for product in product_data:
            self.collect_data(row=product, data=formatted_data, seen_articul=articul_list)

        self.save_data_with_tabs(data=formatted_data, filename=self.plu_file_path)
        write_log_file("plu file successfully created.")

    def collect_data(self, row: tuple, data: list, seen_articul: list):
        unit_param = self.units_dict[row[3]]
        product_price = int(row[4])
        articul = validate_unique_integer_string(row[5], seen_articul) if self.use_articul else False
        lf_code = articul if articul else row[1]
        hotkey = row[5] if self.use_description_as_hotkey and row[5] else 0

        collected_row = [
            hotkey, # Hotkey
            row[2], # Name
            lf_code, # LFCode
            row[1], # Code
            unit_param["barcode_type"], # Barcode Type
            product_price, # Unit Price
            unit_param["scale_unit_id"], # Unit Weight
            unit_param["prefix"],  # Department
            0,  # PT Weight
            15,  # Shelf Time
            0,  # Pack Type
            0,  # Tare
            0, # Error(%)
            0, # Message1
            0, # Message2
            0,
            unit_param["label_id"],  # Label
            0, # Discount/Table
            0, # nutrition
        ]
        data.append(collected_row)

update_data = UpdateData()

def main():
    if len(sys.argv) > 1:
        if sys.argv[1] == "service":
            write_log_file("Starting in service mode...")
            while True:
                try:
                    if not update_data.is_mysql_connected:
                        update_data.connect_mysql()
                    else:
                        update_data.create_plu_file()

                    time.sleep(update_data.check_time)
                except Exception as e:
                    write_log_file(f"Service error: {str(e)}")
                    time.sleep(60)  # Wait a bit before retrying if there's an error
        else:
            write_log_file(f"Not valid argument was passed: {sys.argv[1]}")
            write_log_file(f"Invalid argument: {sys.argv[1]}. Use 'service' to run in service mode.")
    else:
        # No arguments - run once
        write_log_file("Running one-time PLU file creation...")
        update_data.create_plu_file()

if __name__ == "__main__":
    main()