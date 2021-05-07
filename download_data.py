import requests
import tarfile
import zipfile
from multiprocessing.pool import ThreadPool as Pool
import os

BASE_DIR = "C:\\Users\\Denis\\Desktop\\Licenta-LipReading\\datasets\\"

BASE_REQUEST_PATH = "http://spandh.dcs.shef.ac.uk/gridcorpus/"

pool_size = 33 * 3


def download_file(url, dir_to_write):
    local_filename = dir_to_write + url.split('/')[-1]
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return local_filename


def get_lowres_vids(speaker):
    url = BASE_REQUEST_PATH + "s" + str(speaker) + "/video/s" + str(speaker) + ".mpg_vcd.zip"
    if speaker > 20:
        out_dir = BASE_DIR + "low\\" + "\\s" + str(speaker - 1) + "\\"
    else:
        out_dir = BASE_DIR + "low\\" + "\\s" + str(speaker) + "\\"

    os.makedirs(out_dir, exist_ok=True)

    zip_fname = download_file(url, dir_to_write=out_dir)
    with zipfile.ZipFile(zip_fname, 'r') as zip_ref:
        for zip_info in zip_ref.infolist():
            if zip_info.filename[-1] == '/':
                continue
            zip_info.filename = os.path.basename(zip_info.filename)
            zip_ref.extract(zip_info, out_dir)
    os.remove(zip_fname)


def get_hires_vids(speaker):
    url1 = BASE_REQUEST_PATH + "s" + str(speaker) + "/video/s" + str(speaker) + ".mpg_6000.part1.tar"
    url2 = BASE_REQUEST_PATH + "s" + str(speaker) + "/video/s" + str(speaker) + ".mpg_6000.part2.tar"

    if speaker > 20:
        out_dir = BASE_DIR + "high\\" + "\\s" + str(speaker - 1) + "\\"
    else:
        out_dir = BASE_DIR + "high\\" + "\\s" + str(speaker) + "\\"

    os.makedirs(out_dir, exist_ok=True)

    tar_fname1 = download_file(url1, dir_to_write=out_dir)
    tar_fname2 = download_file(url2, dir_to_write=out_dir)
    with tarfile.open(tar_fname1) as tar_ref1:
        for member in tar_ref1.getmembers():
            if member.isreg():
                member.name = os.path.basename(member.name)
                tar_ref1.extract(member, out_dir)
    with tarfile.open(tar_fname2) as tar_ref2:
        for member in tar_ref2.getmembers():
            if member.isreg():
                member.name = os.path.basename(member.name)
                tar_ref2.extract(member, out_dir)
    os.remove(tar_fname1)
    os.remove(tar_fname2)


def get_aligns(speaker):
    url = BASE_REQUEST_PATH + "s" + str(speaker) + "/align/s" + str(speaker) + ".tar"
    if speaker > 20:
        out_dir = BASE_DIR + "word-alignments\\" + "\\s" + str(speaker - 1) + "\\"
    else:
        out_dir = BASE_DIR + "word-alignments\\" + "\\s" + str(speaker) + "\\"

    os.makedirs(out_dir, exist_ok=True)

    tarfilename = download_file(url, dir_to_write=out_dir)
    with tarfile.open(tarfilename) as tarref:
        for member in tarref.getmembers():
            if member.isreg():
                member.name = os.path.basename(member.name)
                tarref.extract(member, out_dir)
    os.remove(tarfilename)

pool = Pool(pool_size)

for idx in range(34):
    if idx != 20:
        pool.apply_async(get_lowres_vids, (idx + 1,))
        pool.apply_async(get_hires_vids, (idx + 1,))
        pool.apply_async(get_aligns, (idx + 1,))


pool.close()
pool.join()

