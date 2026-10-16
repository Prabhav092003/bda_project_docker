"""
hdfs_client.py
Minimal WebHDFS (REST) client built on `requests` - no Hadoop installation needed in the app
container. Used only when WEBHDFS_URL is set (docker compose --profile cluster); otherwise the
project writes to the local, HDFS-style partitioned directory tree under hdfs_storage/.
"""

import os
import sys
import requests

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

HDFS_USER = os.getenv('HDFS_USER', 'root')


def is_available(timeout=2):
    """True when a real HDFS NameNode answers on its WebHDFS endpoint."""
    if not config.WEBHDFS_URL:
        return False
    try:
        r = requests.get(f"{config.WEBHDFS_URL}/webhdfs/v1/", params={'op': 'LISTSTATUS', 'user.name': HDFS_USER}, timeout=timeout)
        return r.status_code == 200
    except Exception:
        return False


def upload_file(local_path, hdfs_path):
    """Upload (overwrite) a local file to HDFS via WebHDFS two-step CREATE."""
    url = f"{config.WEBHDFS_URL}/webhdfs/v1{hdfs_path}"
    params = {'op': 'CREATE', 'overwrite': 'true', 'user.name': HDFS_USER}
    step1 = requests.put(url, params=params, allow_redirects=False, timeout=10)
    if step1.status_code != 307:
        raise RuntimeError(f"NameNode refused CREATE ({step1.status_code}): {step1.text[:200]}")
    with open(local_path, 'rb') as f:
        step2 = requests.put(step1.headers['Location'], data=f, timeout=60)
    if step2.status_code not in (200, 201):
        raise RuntimeError(f"DataNode write failed ({step2.status_code}): {step2.text[:200]}")
    return True
