#!usr/bin/env python
# -*-coding:utf-8-*-
"""
author:wubaichuan

"""
import paramiko
import os
import stat
import re
import threading
import Queue
from logger import log
import sys


class SftpHelper(object):
    def __init__(self, host, username, passwd, port=22):
        self.host = host
        self.port = port
        self.username = username
        self.passwd = passwd
        self.t = paramiko.Transport((self.host, self.port))
        self.connect()
        self.sftp = paramiko.SFTPClient.from_transport(self.t)

    def connect(self):
        self.t.connect(username=self.username, password=self.passwd)


class FileHandler(object):
    def __init__(self, host, username, passwd, local_dir, remote_dir, port=22):
        self.host = host
        self.port = port
        self.username = username
        self.passwd = passwd
        self.local_dir = local_dir
        self.remote_dir = remote_dir
        self.t = paramiko.Transport((self.host, self.port))
        self.connect()
        self.sftp = paramiko.SFTPClient.from_transport(self.t)
        self._queue = Queue.Queue()

    def connect(self):
        self.t.connect(username=self.username, password=self.passwd)

    def get_one(self, sftp_helper, remote_file):
        pat = re.compile(self.remote_dir)
        local_file = pat.sub(self.local_dir, remote_file)
        local_dir = os.path.dirname(local_file)
        if not os.path.exists(local_dir):
            try:
                os.makedirs(local_dir)
            except OSError:
                pass
        sftp_helper.sftp.get(remote_file, local_file)
        log.info('Download %s successfully' % local_file)
        sftp_helper.sftp.remove(remote_file)
        log.info('remove %s successfully' % remote_file)
        self._queue.put(sftp_helper, timeout=1)

    def get_file_list(self, remote_dir):
        files = self.sftp.listdir_attr(remote_dir)
        if not files:
            self.sftp.rmdir(remote_dir)
            log.info('delete empty dir %s' % remote_dir)
        for f in files:
            filename = remote_dir + '/' + f.filename
            if stat.S_ISDIR(f.st_mode):
                for i in self.get_file_list(filename):
                    yield i
            else:
                yield filename

    def downloader_async(self, remote_dir):
        threads = []
        for i in range(10):
            self._queue.put(SftpHelper(self.host, self.username, self.passwd))
        for i in self.get_file_list(remote_dir):
            try:
                sftp_helper = self._queue.get(timeout=20)
            except Queue.Empty:
                log.warn('No sftp is available')
                break
            thr = threading.Thread(target=self.get_one, args=(sftp_helper, i))
            threads.append(thr)
            thr.start()
            if len(threads) > 10:
                for t in threads:
                    t.join()
                threads = []
        for t in threads:
            t.join()
        while not self._queue.empty():
            i = self._queue.get()
            i.sftp.close()
            i.t.close()
        self.sftp.close()
        self.t.close()


if __name__ == '__main__':
    host_a = sys.argv[1]
    username_a = sys.argv[2]
    passwd_a = sys.argv[3]
    local_dir_a = sys.argv[4]
    remote_dir_a = sys.argv[5]
    FileHandler(host_a, username_a, passwd_a, local_dir_a, remote_dir_a).downloader_async(remote_dir_a)
