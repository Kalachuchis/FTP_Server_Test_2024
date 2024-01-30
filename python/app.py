import argparse
import ftplib
import io
import os
import zipfile

from src import DirectoryParser, FileCloudController
from src.directory_parser import RemoteExtraction
from src.utils import bcolors, timerdecorator


def parse_opt():
    parser = argparse.ArgumentParser()
    parser.add_argument("--remote", nargs="?", const=True, default=False)
    parser.add_argument("--ftp_server", type=str, default="")
    parser.add_argument("--ftp_user", type=str, default="")
    parser.add_argument("--ftp_passwd", type=str, default="")
    parser.add_argument("--zip_path", type=str, default="")
    parser.add_argument("--contract", type=str, default="")

    return parser.parse_args()


@timerdecorator
def main(opt):
    # Log in to Filecloud
    server_url = "http://40.78.9.249"
    fc_service = FileCloudController(server_url, "test", "Pointwest!2345678")
    fc_service.login()
    fc_service.admin_login()

    # Check directory for output zipfiles or directly go to zip location
    # Then extracts KVP and PDF files of zipfile/s
    print(bcolors.OKCYAN + f"Checking Directory:" + bcolors.ENDC, end=" ")
    print(bcolors.UNDERLINE + opt.ftp_server + bcolors.ENDC)
    file_parser = DirectoryParser(
        src_path=opt.ftp_server,
        zip_path=opt.zip_path,
        contract=opt.contract,
    )

    if opt.remote:
        # Check if complete arguments
        ftp_infos = [opt.ftp_server, opt.ftp_user, opt.ftp_passwd]
        if not all(ftp_infos):
            print(bcolors.FAIL + "Missing arguments" + bcolors.ENDC)
            return
        file_parser.extraction_strat = RemoteExtraction(
            opt.ftp_server,
            opt.ftp_user,
            opt.ftp_passwd,
        )

    # Walks through directory
    kvp = file_parser.process_dir()
    print(kvp)

    print(bcolors.OKCYAN + "Uploading to Filecloud...." + bcolors.ENDC)
    if kvp:
        for job in kvp:
            for element in kvp[job]:
                print(element)
                filename = element
                file_data = kvp[job][element]["PDF File"]
                file_kvp = (
                    kvp[job][element]["KVP"] if "KVP" in kvp[job][element] else None
                )

                fc_service.upload_file(filename, file_data, file_kvp)

    """ 
    test writing file stored in object

    print(bcolors.OKCYAN + "Extracted KVP:" + bcolors.ENDC)
    print(json.dumps(kvp, indent=4))

    with open("test.pdf", "wb") as f:
        f.write(kvp["BILL PLDT 1P"]["PDF File"].read())
    """


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


@timerdecorator
def lambda_handler(event):
    print(bcolors.OKCYAN + f"Checking Directory:" + bcolors.ENDC, end=" ")
    server_url = "http://40.78.9.249"
    fc_service = FileCloudController(server_url, "test", "Pointwest!2345678")
    fc_service.login()
    fc_service.admin_login()

    contract = event["repository_path"].split("/")[0]
    job = event["job_code"]
    file_parser = DirectoryParser(
        src_path=event["repository_path"],
        contract=contract,
        job=event["job_code"],
    )

    file_parser.extraction_strat = RemoteExtraction(
        event["host"], event["username"], event["password"]
    )
    kvp = file_parser.process_job()
    if kvp:
        kvp_job = kvp[job]
        for element in kvp_job:
            print(element)
            if element == "FAILED":
                for file in kvp[job][element]:
                    filename = f"({job}) {file.name}"
                    fc_service.upload_file(filename, file, kvp=None, is_failed=True)
                    print(file)
                    print(file.name)
            else:
                filename = element
                file_data = kvp_job[element]["PDF File"]
                file_kvp = (
                    kvp_job[element]["KVP"] if "KVP" in kvp_job[element] else None
                )

                fc_service.upload_file(filename, file_data, file_kvp)


if __name__ == "__main__":
    opt = parse_opt()
    main(opt)
