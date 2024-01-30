import ftplib
import io
import os
import platform
import re
import zipfile
from abc import ABC, abstractmethod
from typing import Dict, List, Tuple

import pandas as pd

from .config import *
from .utils import bcolors, timerdecorator

system = platform.system()
MOCK_FTP = (
    "/mnt/d/pointwest/BPICT/Mock_FTP/"
    if system == "Linux"
    else r"D:\pointwest\BPICT\Mock_FTP"
)


# Needed to connect to new FTP server
class MyFTP_TLS(ftplib.FTP_TLS):
    """Explicit FTPS, with shared TLS session"""

    def ntransfercmd(self, cmd, rest=None):
        conn, size = ftplib.FTP.ntransfercmd(self, cmd, rest)
        if self._prot_p:
            conn = self.context.wrap_socket(
                conn, server_hostname=self.host, session=self.sock.session
            )  # this is the fix
        return conn, size


class ExtractionStrategy(ABC):
    """Strategy for extracting zipfile from FTP server"""

    @abstractmethod
    def _get_output_folders(
        self,
        folder: str,
        source_dir: str | None = None,
    ) -> List[str]:
        pass

    @abstractmethod
    def _get_failed(self, folder: str, job: str):
        pass

    @abstractmethod
    def get_zip_output(self, file_path: str) -> zipfile.ZipFile:
        pass

    @abstractmethod
    def get_zip_files(self, output_dir: str) -> List[str]:
        pass


class RemoteExtraction(ExtractionStrategy):
    def __init__(self, server, user, passwd):
        try:
            self.ftp = MyFTP_TLS()
            """
            self.ftp.connect(os.environ["FTP_SERVER"], port=21)
            self.ftp.login(user=os.environ["FTP_USER"], passwd=os.environ["FTP_PASS"])
            """
            # self.ftp.set_debuglevel(2)
            self.ftp.connect(server, port=21)
            self.ftp.login(user=user, passwd=passwd)
            self.ftp.prot_p()
            self.ftp.cwd("~")

            self.ftp.encoding = "utf-8"
        except ftplib.error_perm as e:
            self.ftp = ftplib.FTP()
            """
            self.ftp.connect(os.environ["FTP_SERVER"], port=21)
            self.ftp.login(user=os.environ["FTP_USER"], passwd=os.environ["FTP_PASS"])
            """
            # self.ftp.set_debuglevel(2)
            self.ftp.connect(server, port=21)
            self.ftp.login(user=user, passwd=passwd)
            self.ftp.cwd("~")

            self.ftp.encoding = "utf-8"

    def _get_output_folders(
        self,
        folder: str,
        source_dir: str | None = None,
    ) -> List[str]:
        dir_regex = r"^[\(a-zA-Z0-9-_\s)+\/]+$"
        skip_regex = [
            r"Input",
            r"Catalogued",
            r"Invalid",
            r"Failed",
            "For Processing",
        ]
        output_regex = r"\/Output"
        entries = []
        output_dirs = []

        # _folder = f"/{folder}" if not folder.startswith("/") else folder
        print(folder)
        if folder:
            entries.extend(self.ftp.nlst(folder))
        else:
            entries.extend(self.ftp.nlst())

        for entry in entries:
            if any([re.search(regex, entry) for regex in skip_regex]):
                continue
            if re.search(dir_regex, entry):
                if re.search(output_regex, entry):
                    return self.ftp.nlst(entry)
                output_dirs.extend(self._get_output_folders(entry))

        return output_dirs

    def _get_failed(
        self,
        contract: str,
        job: str,
    ) -> List[str]:
        """
        Inside failed files/job and invalid files/job should contain a folder
        with the vehicle name as the folder name and the failed/invalid pdf files
        as its' content.
        """
        entries = []
        failed = []
        failed_path = f"{contract}/{FAILED_FOLDER}/{job}"
        invalid_path = f"{contract}/{INVALID_FOLDER}/{job}"

        entries.extend(self.ftp.nlst(failed_path))
        entries.extend(self.ftp.nlst(invalid_path))

        # Loops through each vehicle folder per failed job folder
        for _vehicle_folder in entries:
            for file in self.ftp.nlst(_vehicle_folder):
                tree = file.split("/")
                ftp_stream = io.BytesIO()
                # with open(f"{tree[-1]}", "rb+") as ftp_stream:
                self.ftp.retrbinary(f"RETR {file}", ftp_stream.write)
                ftp_stream.name = tree[-1]
                failed.append(ftp_stream)

        return failed

    def get_zip_output(self, file_path: str) -> zipfile.ZipFile | None:
        """Extracts bytes from FTP server and convert into io Bytes so zipfile lib can read
        zip file"""

        print("inside get_zip_output")
        print(file_path)
        zip_path = self.ftp.nlst(file_path)
        print(zip_path)
        ftp_stream = io.BytesIO()
        if zip_path:
            print(f"retrieving {zip_path[0]} from ftp")
            self.ftp.retrbinary(f"RETR {zip_path[0]}", ftp_stream.write)
            output_zip = zipfile.ZipFile(ftp_stream)

            return output_zip

    def get_zip_files(self, output_dir):
        zip_files = []
        for entry in self.ftp.nlst(output_dir):
            if entry.endswith("zip"):
                zip_files.extend([entry])
            else:
                zip_files.extend(self.get_zip_files(entry))

        return zip_files


class LocalExtraction(ExtractionStrategy):
    def _get_output_folders(
        self,
        folder: str,
        source_dir: str | None = None,
    ) -> List[str]:
        output_dirs = []
        # folders containing these characters are not important
        skip_regex = [
            r"input",
            r"Catalogued",
            r"Invalid",
            r"Failed",
            "For Processing",
        ]
        folder = os.path.join(source_dir, folder) if source_dir else folder
        for entry in os.scandir(folder):
            if entry.is_dir():
                if any([re.search(regex, entry.name) for regex in skip_regex]):
                    continue
                if entry.name == "output":
                    return [entry.path]
                output_dirs.extend(self._get_output_folders(folder=entry.path))

        return output_dirs

    def _get_failed(
        self,
        folder: str,
        source_dir: str | None = None,
    ) -> List[str]:
        output_dirs = []
        # folders containing these characters are not important
        skip_regex = [
            r"input",
            r"Catalogued",
            r"Invalid",
            r"Failed",
            "For Processing",
            "output",
        ]

        failed_regex = [
            r"Invalid",
            r"Failed",
        ]
        folder = os.path.join(source_dir, folder) if source_dir else folder
        for entry in os.scandir(folder):
            if entry.is_dir():
                if any([re.search(regex, entry.name) for regex in skip_regex]):
                    continue
                if any([re.search(regex, entry.name) for regex in failed_regex]):
                    return [entry.path]
                output_dirs.extend(self._get_output_folders(folder=entry.path))

        return output_dirs

    def get_zip_output(self, file_path: str) -> zipfile.ZipFile:
        """Extract zip from local path"""
        output_zip = zipfile.ZipFile(file_path)
        return output_zip

    def get_zip_files(self, output_dir):
        zip_files = []
        for entry in os.scandir(output_dir):
            if entry.is_dir():
                zip_files.extend(self.get_zip_files(entry.path))
            if entry.is_file() and entry.path.endswith("zip"):
                zip_files.extend([entry.path])

        return zip_files


class DirectoryParser:
    excel_regex = r"^.+\/.+\.xlsx$"
    searchable_pdf_regex = r"Searchable PDF"
    kvp_per_file = {}

    def __init__(
        self,
        src_path: str = "",
        zip_path: str = "",
        contract: str = "",
        job: str = "",
    ):
        if zip_path == "" and src_path == "":
            print(
                bcolors.WARNING
                + f"No specified path provided. Will use default: {MOCK_FTP}"
                + bcolors.ENDC
            )
            self.src_path = MOCK_FTP
            self.zip_path = ""

        else:
            self.src_path = src_path
            self.zip_path = zip_path

        self.contract = contract
        self.job = job
        self._extraction_strat = LocalExtraction()

    @property
    def extraction_strat(self) -> ExtractionStrategy:
        return self._extraction_strat

    @extraction_strat.setter
    def extraction_strat(self, strategy: ExtractionStrategy):
        self._extraction_strat = strategy

    @timerdecorator
    def _extract_from_zip(
        self,
        zip_file_path: str,
        batch_name: str,
    ) -> List[Tuple[pd.ExcelFile, str] | None]:
        """Extracts useful files from zipfiles. Specifically the searchable pdf file
        uploaded by Skriba and their respective excel files containing their key value pair

        Parameters
        ----------
        self
            Needed here is the source path which is the watched or specified directory containing the zip files

        file
            String indicating filename of zip file to perform extraction


        Returns
        -------
        List[pd.ExcelFile | None]
            List of extracted excel files containing KVP from zip
        """
        self.kvp_per_file[batch_name] = {}
        excel_datas = []
        try:
            """Checks contents of zipfile and filters excel files"""
            print(zip_file_path)
            output_zip = self._extraction_strat.get_zip_output(zip_file_path)
            print(output_zip)
            if not output_zip:
                return excel_datas
            zip_list = output_zip.namelist()

            """Convert excel files found in zip file and converts into pd.ExcelFile"""
            excel_files = [
                file for file in zip_list if re.search(self.excel_regex, str(file))
            ]

            # Used pd.ExcelFile since there are always more than two sheets
            for file in excel_files:
                f = output_zip.open(file)
                # Get only the Batch Spreadsheet excel
                # if re.match(r".*Batch KVP Spreadsheet\.xlsx", f.name):
                if re.match(BATCH_SPREADSHEET_NAME, f.name):
                    _excel = pd.ExcelFile(f)
                    excel_datas.append((_excel, batch_name))

            """Extracts pdf files that are searchable pdfs"""
            searchable_pdf_files = [
                file
                for file in zip_list
                if re.search(self.searchable_pdf_regex, str(file))
            ]

            for file in searchable_pdf_files:
                # _filename = re.sub(".*Searchable PDF/", "", file)
                _filename = re.sub(SUB_SEARCHABLE_REGEX, "", file)
                _filename = file.split("/")[-1]
                # filename = _filename.removesuffix(" - Searchable PDF.pdf")
                filename = _filename.removesuffix(SUFFIX_REMOVE_SEARCHABLE)
                self.kvp_per_file[batch_name][filename] = {}
                f = output_zip.open(file, "r")
                self.kvp_per_file[batch_name][filename]["PDF File"] = f
            """
            # Hopefully walang "output" string sa mga batch names??
            # Can be improved
            processed_path = os.path.dirname(zip_file_path).replace(
                "output", "processed"
            )
            if not os.path.exists(processed_path):
                os.makedirs(processed_path)

            print(os.path.dirname(output_dir))
            print(processed_path)
            # shutil.move(file_path, processed_path)
            """
            return excel_datas
        except zipfile.BadZipFile as e:
            print(bcolors.FAIL + f"{zip_file_path}: {e}" + bcolors.ENDC)
            return excel_datas

    def _parse_dataframe(self, excel: pd.ExcelFile, job):
        """Takes in an excel file in the "KVP Excel File" Folder and turns the key value pairings
        into a python object and stores it in the kvp_per_file attribute"""

        sheet_names = excel.sheet_names
        """Check whether the excel file is the summary file"""
        if len(sheet_names) < 2:
            return

        ## Baka di na kailangan to since may filename na sa bawat page
        summary_df = excel.parse(sheet_names[0])
        filename_template_pairings = {}
        if isinstance(summary_df, pd.DataFrame):
            df_records = summary_df[summary_df.columns[0:3]].to_dict("records")
            for record in df_records:
                # Columns needed from summary sheet to connect kvp to pdf file
                sum_doc_type = record["Document Type"]
                sum_page = record["Page number"]
                sum_file_name = record["File name"]
                if sum_doc_type not in filename_template_pairings:
                    filename_template_pairings[sum_doc_type] = {}

                filename_template_pairings[sum_doc_type][sum_page] = sum_file_name

        for sheet_name in sheet_names[1:]:
            df = excel.parse(sheet_name)
            if isinstance(df, pd.DataFrame):
                if df.empty:
                    continue

                """Slices the DF, chooses only columns for template and page"""
                # _template_df = df.iloc[:, :2]
                _template_df = df.iloc[:, :3]

                """Slices the DF, chooses only columns for key value pairing"""
                # _kvp_df = df.iloc[:, 5::2]
                _kvp_df = df.iloc[:, 6::2]
                kvp_df = pd.concat([_template_df, _kvp_df], axis=1)
                kvp_records = kvp_df.to_dict("records")

                for kvp_record in kvp_records:
                    """
                    template = kvp_record["Template"]
                    page = kvp_record["Page"]
                    filename = filename_template_pairings[template][page]
                    """
                    filename = kvp_record["File name"]
                    if filename in self.kvp_per_file[job]:
                        """
                        self.kvp_per_file[filename]["KVP"] = kvp_record
                        """
                        if "KVP" not in self.kvp_per_file[job][filename]:
                            self.kvp_per_file[job][filename]["KVP"] = []
                        self.kvp_per_file[job][filename]["KVP"].extend([kvp_record])

    @timerdecorator
    def process_dir(self) -> Dict:
        """Main Process for this class. Starts the process for parsing the ouput zip folders
        and converts into ??? for uploading to filecloud"""
        print(
            bcolors.OKCYAN + f"Walking through FTP Server. Please Wait" + bcolors.ENDC,
        )
        excel_files = []

        try:
            foldername = self.contract if self.contract else ""
            output_dirs = self.extraction_strat._get_output_folders(
                folder=foldername,
                source_dir=self.src_path,
            )
            if output_dirs:
                for dir in output_dirs:
                    for zip_file in self.extraction_strat.get_zip_files(dir):
                        print(zip_file)
                        batch_name = os.path.split(zip_file)[1].removesuffix(".zip")
                        excel_files.extend(self._extract_from_zip(zip_file, batch_name))
                        """
                        failed_dirs = self.extraction_strat._get_failed(
                            folder=foldername,
                            job=batch_name,
                        )
                        """
            else:
                # Change to Exception??
                print(bcolors.FAIL + f"{self.src_path} is Empty" + bcolors.ENDC)
                return self.kvp_per_file

            if excel_files:
                for excel, job in excel_files:
                    self._parse_dataframe(excel, job)

            return self.kvp_per_file
        except IsADirectoryError as e:
            print(bcolors.FAIL + f"{e}" + bcolors.ENDC)
            return self.kvp_per_file
        except FileNotFoundError as e:
            print(bcolors.FAIL + f"{e}" + bcolors.ENDC)
            print(
                bcolors.FAIL
                + f"Please check paths and contract name if it exists"
                + bcolors.ENDC
            )
            return self.kvp_per_file

    @timerdecorator
    def process_job(self) -> Dict | None:
        excel_files = []
        path = f"{self.src_path}/Job {self.job}"
        try:
            excel_files.extend(self._extract_from_zip(path, self.job))

            if excel_files:
                for excel, job in excel_files:
                    self._parse_dataframe(excel, job)

            """
            failed_stuff = self.extraction_strat._get_failed(self.contract, self.job)
            print(failed_stuff)
            self.kvp_per_file[self.job]["FAILED"] = failed_stuff
            """
            return self.kvp_per_file
        except IsADirectoryError as e:
            print(bcolors.FAIL + f"{e}" + bcolors.ENDC)
        except FileNotFoundError as e:
            print(bcolors.FAIL + f"{e}" + bcolors.ENDC)
            print(
                bcolors.FAIL
                + f"Please check paths and contract name if it exists"
                + bcolors.ENDC
            )
