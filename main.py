#!/usr/bin/python3
import os
import configparser
import logging
import datetime

logging_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'run.log')
logging.basicConfig(filename=logging_file, level=logging.INFO,
                    format='%(asctime)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M')


class MysqlBackup(object):
    """Mysql备份、异地备份、发送邮件"""
    def __init__(self):
        config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini')
        config = configparser.ConfigParser()
        config.read(config_file, encoding='utf-8')

        assert config.has_section("BACKUP"), '缺少BACKUP配置项'

        self.mysql_user = config.get('BACKUP', 'user')
        self.mysql_password = config.get('BACKUP', 'password')
        self.local_backup_path = config.get('BACKUP', 'backup_path')
        try:
            self.mysql_host = config.get('BACKUP', 'host')
        except configparser.NoOptionError:
            self.mysql_host = 'localhost'
        try:
            self.mysql_port = config.get('BACKUP', 'port')
        except configparser.NoOptionError:
            self.mysql_port = 3306
        try:
            self.mysqldump_path = config.get('BACKUP', 'mysqldump_path')
        except configparser.NoOptionError:
            self.mysqldump_path = '/usr/bin/mysqldump'
        try:
            self.innobackupex_path = config.get('BACKUP', 'innobackupex_path')
        except configparser.NoOptionError:
            self.innobackupex_path = '/usr/bin/innobackupex'
        try:
            self.my_cnf = config.get('BACKUP', 'my_cnf')
        except configparser.NoOptionError:
            self.my_cnf = '/etc/my.cnf'

        self.is_send_mail = False
        if config.has_section('MAIL'):
            self.smtp_host = config.get('MAIL', 'host')
            self.smtp_port = config.get('MAIL', 'port')
            self.smtp_user = config.get('MAIL', 'user')
            self.smtp_password = config.get('MAIL', 'password')
            self.smtp_from = config.get('MAIL', 'from')

            self.receivers = []
            try:
                to = config.get('MAIL', 'to')
            except configparser.NoOptionError:
                logging.info('未配置收件人，将不会发送邮件')
            else:
                for receiver in to.split(','):
                    self.receivers.append(receiver.strip())
                if len(self.receivers):
                    self.is_send_mail = True
        else:
            logging.info('未发现SMTP配置，将不会发送邮件')

        if config.has_section('REMOTE'):
            self.is_remote_backup = True
            self.ssh_host = config.get('REMOTE', 'host')
            self.ssh_user = config.get('REMOTE', 'user')
            self.remote_backup_path = config.get('REMOTE', 'backup_path')
            try:
                self.ssh_port = config.get('REMOTE', 'port')
            except configparser.NoOptionError:
                self.ssh_port = 22
            try:
                self.ssh_password = config.get('REMOTE', 'password')
            except configparser.NoOptionError:
                # 已配置免密登录
                self.ssh_password = None
        else:
            self.is_remote_backup = False
            logging.info('未发现REMOTE配置，将不会进行异地备份')

        # 创建本次备份文件夹
        self.backup_dir = str(datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
        #os.mkdirs(os.path.join(self.local_backup_path, self.backup_dir))
        print(os.path.join(self.local_backup_path, self.backup_dir))

    def structure_backup(self):
        dump_file = os.path.join(self.local_backup_path, self.backup_dir, 'database.sql')
        print('{} -u {} -p {} -h {} -P {} --all-databases -d > {}'.format(
            self.mysqldump_path,
            self.mysql_user,
            self.mysql_password,
            self.mysql_host,
            self.mysql_port,
            dump_file
        ))

    def full_backup(self):
        dump_dir = os.path.join(self.local_backup_path, self.backup_dir, 'databases')
        print('{} --defaults -file={} --host={} --port={} --user={} --password={} --no-timestamp {}'.format(
            self.innobackupex_path,
            self.my_cnf,
            self.mysql_host,
            self.mysql_port,
            self.mysql_user,
            self.mysql_password,
            dump_dir
        ))

    def run(self):
        self.structure_backup()
        self.full_backup()


if __name__ == '__main__':
    mb = MysqlBackup()
    mb.run()
