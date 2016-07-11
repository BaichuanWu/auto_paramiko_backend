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
        self.rt = paramiko.Transport((self.host, self.port))
        self.rconnect()
        self.rsftp = paramiko.SFTPClient.from_transport(self.rt)
        self._queue = Queue.Queue()

    def connect(self):
        self.t.connect(username=self.username, password=self.passwd)

    def rconnect(self):
        self.rt.connect(username=self.username, password=self.passwd)

    @staticmethod
    def delete_all(sftp_helper, filename, remote_dir):
        if filename == remote_dir:
            return 0
        if not sftp_helper.sftp.listdir(filename):
            sftp_helper.sftp.rmdir(filename)
            log.info('***remove - delete empty dir %s' % filename)
            filename = os.path.dirname(filename)
            FileHandler.delete_all(sftp_helper, filename)

    def get_one(self, sftp_helper, remote_file):
        pat = re.compile(self.remote_dir)
        local_file = pat.sub(self.local_dir, remote_file)
        local_dir = os.path.dirname(local_file)
        if not os.path.exists(local_dir):
            try:
                os.makedirs(local_dir)
            except OSError:
                pass
        log.debug('Start download %s -------' % remote_file)
        try:
            sftp_helper.sftp.get(remote_file, local_file)
            log.info('Download %s successfully' % local_file)
            sftp_helper.sftp.remove(remote_file)
            log.info('remove %s successfully' % remote_file)
            remote_dir = os.path.dirname(remote_file)
            try:
                FileHandler.delete_all(sftp_helper, remote_dir, self.remote_dir)
            except Exception as e:
                pass
        except Exception as e:
            log.warn(e)
        self._queue.put(sftp_helper, timeout=1)

    # def get_roor_dir(self):
    #     log.debug('get root dir')
    #     return self.rsftp.listdir_iter(self.remote_dir)

    def get_file_list(self, remote_dir, is_root=False):
        log.debug('now start get list')
        # if is_root:
        #     files = self.get_roor_dir()
        # else:
        #     files = self.sftp.listdir_attr(remote_dir)
        files = self.sftp.listdir_attr(remote_dir)
        for f in files:
            filename = remote_dir + '/' + f.filename
            log.debug('now search %s' % filename)
            if stat.S_ISDIR(f.st_mode):
                for i in self.get_file_list(filename):
                    yield i
            else:
                yield filename

    def downloader_async(self, remote_dir):
        threads = []
        for i in range(20):
            log.debug('-------connecting %s' % i)
            self._queue.put(SftpHelper(self.host, self.username, self.passwd))
        log.debug('stop connect')
        for i in self.get_file_list(remote_dir, is_root=True):
            log.debug('%%%%')
            try:
                sftp_helper = self._queue.get(timeout=1)
                log.debug('------get %s' % i)
            except Queue.Empty:
                log.warn('No sftp is available')
                sftp_helper = SftpHelper(self.host, self.username, self.passwd)
                log.info('add new sftp')
            thr = threading.Thread(target=self.get_one, args=(sftp_helper, i))
            threads.append(thr)
            thr.start()
            if len(threads) > 40:
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
