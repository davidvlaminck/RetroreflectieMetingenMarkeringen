import csv
import zipfile
from pathlib import Path

import pandas
from pandas import DataFrame


def walk_through_dirs(start_path: Path):
    for file in start_path.rglob('*'):
        if not file.is_dir() and file.suffix == '.xls':
            yield file

def main(drive_path: Path, output_dir_path: Path):
    if not output_dir_path.exists():
        output_dir_path.mkdir()

    combined_df = DataFrame()
    for file_path in walk_through_dirs(drive_path):
        processed_df = process_one_file(output_dir_path, file_path)
        if processed_df is not None:
            combined_df = pandas.concat([combined_df, processed_df], ignore_index=True, sort=False)

    combined_df = combined_df[combined_df['RL'] != '0']
    print(combined_df)
    combined_df.to_csv(output_dir_path / 'combined.csv', sep=';', index=False)

def process_one_file(output_dir_path: Path, file_path: Path) -> DataFrame:
    file_name = file_path.name
    file_name_first_part = file_name.split(' ')[0]
    dir_name = file_path.parent.name
    if file_name_first_part != dir_name:
        print(f'name and directory name not conform: {file_name}, {dir_name}')
        return
    csv_path = output_dir_path / file_name.replace('.xls', '.csv')
    with open(file_path, encoding='ISO-8859-1') as csv_file:
        reader = csv.reader(csv_file, delimiter='\t', quoting=csv.QUOTE_NONE, escapechar='\\')
        with open(csv_path, 'w+', encoding='ISO-8859-1') as output_file:
            writer = csv.writer(output_file, delimiter=';', quoting=csv.QUOTE_NONE, escapechar='\\')

            reading_metadata = True
            reading_header = False
            metadata_dict = {}
            data, data_headers = [], []
            ident8, measure_series_name, markering = '', '', ''

            for row in reader:
                row = [r.replace('"', '').replace(';', '') for r in row]
                if reading_header:
                    reading_header = False
                    row[10] = 'Speed km/h km/h'
                    data_headers = row
                    writer.writerow(row)
                    continue
                writer.writerow(row)
                if reading_metadata and row[0].strip() == '':
                    reading_metadata = False
                    reading_header = True
                    measure_series_name = metadata_dict['Measure Series Name:']
                    ident8 = measure_series_name[:8]
                    markering = measure_series_name[9:11]
                    continue

                if reading_metadata:
                    metadata_dict[row[0]] = row[1]
                else:
                    row.insert(0, markering)
                    row.insert(0, ident8)
                    row.insert(0, measure_series_name)
                    data.append(row[:19])

            data_headers.insert(0, 'markering')
            data_headers.insert(0, 'ident8')
            data_headers.insert(0, 'meting_serie_naam')

            measure_series_name = metadata_dict['Measure Series Name:']

            if file_path.stem != measure_series_name:
                print(f'measure series and name not conform: {measure_series_name}, {file_path.stem}')
                return

            df = DataFrame(data, columns=data_headers[:19])

    zip_path = output_dir_path / f'{file_path.stem}.zip'
    zipfile.ZipFile(zip_path, mode='w').write(csv_path, arcname=csv_path.name)

    return df


if __name__ == '__main__':
    main(drive_path=Path(__file__).parent / 'excel_files', output_dir_path=Path(__file__).parent / 'output')