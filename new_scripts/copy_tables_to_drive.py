from DriveWrapper import DriveWrapper
from PostGISConnector import PostGISConnector


def copy_tables_to_drive(wrapper: DriveWrapper, connector: PostGISConnector, report_year: int,
                         dir_id_root_meetjaar: str):
    # create XLS with sheets for each table
    # copy the xls file to drive
    # https://openpyxl.readthedocs.io/en/stable/tutorial.html
    # https://openpyxl.readthedocs.io/en/stable/usage.html
    raise NotImplementedError
