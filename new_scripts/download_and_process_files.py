import os
import zipfile
from pathlib import Path


def download_and_process_files(connector, wrapper, root_meetjaar, report_year):
    with connector.main_connection.cursor() as cursor:
        while True:
            cursor.execute(f'SELECT * FROM ttw.log_files_state WHERE report_year = {report_year} AND '
                           f'name_conform IS NULL AND completed_step = 3 LIMIT 1;')
            record = cursor.fetchone()
            if record is None:
                break
            file_id = record[2]
            file_name = record[1]
            wrapper.download_file(file_id=file_id, file_path=Path(f'temp/{file_name}'))
            print(record)

            if not os.path.exists(Path('temp')):
                os.mkdir(Path('temp'))
            if not os.path.exists(Path('wdb')):
                os.mkdir(Path('wdb'))

            csv_path = Path(f'temp/{file_name}'.replace('.xls', '.csv'))
            import csv
            with open(Path(f'temp/{file_name}'), encoding='ISO-8859-1') as csv_file:
                with open(csv_path, 'w+', encoding='ISO-8859-1') as output_file:
                    reader = csv.reader(csv_file, delimiter='\t', quoting=csv.QUOTE_NONE, escapechar='\\')
                    writer = csv.writer(output_file, delimiter=';', quoting=csv.QUOTE_NONE, escapechar='\\')

                    reading_headers = True
                    headers_dict = {}
                    data = []
                    for row in reader:
                        row = [r.replace('"', '').replace(';', '') for r in row]
                        writer.writerow(row)
                        if reading_headers and row[0].strip() == '':
                            reading_headers = False
                            continue

                        if reading_headers:
                            headers_dict[row[0]] = row[1]
                        else:
                            data.append(row)

                    data_headers = data[0]

                    measure_series_name = headers_dict['Measure Series Name:']

                    name_conform = True
                    filename = record[1][:-4]
                    if filename != measure_series_name:
                        name_conform = False
                    first_part = measure_series_name.split(' ')[0]
                    if first_part != record[3]:
                        name_conform = False

                    if not name_conform:
                        print(f'name not conform: {measure_series_name}, {record[1]}, {record[3]}')

                    update_query = f"UPDATE ttw.log_files_state " \
                                   f"SET name_conform = {name_conform}, measure_series_name = '{measure_series_name}', " \
                                   f"completed_step = 4 " \
                                   f"WHERE oid = {record[0]}"
                    cursor.execute(update_query)

            zip_path = Path(f'wdb/{file_name}'.replace('.xls', '.zip'))
            zipfile.ZipFile(zip_path, mode='w').write(csv_path, arcname=csv_path.name)

            connector.main_connection.commit()
