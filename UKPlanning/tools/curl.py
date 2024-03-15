import subprocess
from tools.utils import get_temp_storage_path



def upload_file(path):
    # -s or --silent option act as silent or quiet mode. Don’t show progress meter or error messages. Makes Curl mute.
    # -S, --show-error in addition to this option to disable progress meter but still show error messages.
    command = ["curl", "-sS", "-u", f"{user_pass}", "-T", f"{get_temp_storage_path()}{path}", f"https://newfiles.warwick.ac.uk/remote.php/dav/files/{user}/HCNCSync/{path}"]
    return subprocess.call(command)


def upload_folder(path):
    command = ["curl", "-sS", "--insecure", "-u", f"{user_pass}", "-X", "MKCOL", f"https://newfiles.warwick.ac.uk/remote.php/dav/files/{user}/HCNCSync/{path}"]
    return subprocess.call(command)