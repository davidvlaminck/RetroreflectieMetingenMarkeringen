import csv
from pathlib import Path


if __name__ == '__main__':
    dir = Path(__file__).parent.parent / 'temp'
    for file in dir.iterdir():
        if not(file.is_file() and file.suffix == '.csv'):
            continue
        with open(file, encoding='ISO-8859-1') as csv_file:
            with open(str(file).replace('.csv', '_aangepast.csv'), 'w+', encoding='ISO-8859-1') as output_file:
                reader = csv.reader(csv_file, delimiter=';', quoting=csv.QUOTE_NONE, escapechar='\\')
                writer = csv.writer(output_file, delimiter=';', quoting=csv.QUOTE_NONE, escapechar='\\')

                for row in reader:
                    if row[0] == 'Position km':
                        row[10] = 'Speed km/h km/h'
                    writer.writerow(row)


