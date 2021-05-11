#!/usr/bin/python3
import os
import configparser
import logging
import datetime
import tarfile
import shutil
import time
import pexpect
import paramiko

"""
python3 -m pip install -U pip setuptools
"""

logging_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'run.log')
logging.basicConfig(filename=logging_file, level=logging.INFO,
                    format='%(asctime)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M')


def notify():
    pass


class MysqlBackup(object):
    """Mysql备份、异地备份、发送邮件"""
    def __init__(self, config_file=None):
        if not config_file:
            config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini')
            assert os.path.exists(config_file), '未找到config.ini配置文件:{}'.format(config_file)
        config = configparser.ConfigParser()
        config.read(config_file, encoding='utf-8')

        assert config.has_section("BACKUP"), '缺少BACKUP配置项'

        self.mysql_user = config.get('BACKUP', 'user')
        self.mysql_password = config.get('BACKUP', 'password')
        self.local_backup_path = config.get('BACKUP', 'backup_path')
        self.local_reserve_days = int(config.get('BACKUP', 'reserve_days'))
        try:
            self.mysql_host = config.get('BACKUP', 'host')
        except configparser.NoOptionError:
            self.mysql_host = 'localhost'
        try:
            self.mysql_port = int(config.get('BACKUP', 'port'))
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

        if config.has_section('REMOTE'):
            self.is_remote_backup = True
            self.ssh_host = config.get('REMOTE', 'host')
            self.ssh_user = config.get('REMOTE', 'user')
            self.ssh_password = config.get('REMOTE', 'password')
            self.remote_backup_path = config.get('REMOTE', 'backup_path')
            self.remote_reserve_days = int(config.get('REMOTE', 'reserve_days'))
            try:
                self.ssh_port = int(config.get('REMOTE', 'port'))
            except configparser.NoOptionError:
                self.ssh_port = 22
            """
            try:
                self.ssh_password = config.get('REMOTE', 'password')
            except configparser.NoOptionError:
                # 已配置免密登录
                self.ssh_password = None
            """
        else:
            self.is_remote_backup = False
            logging.info('未发现REMOTE配置，将不会进行异地备份')

        # 创建本次备份文件夹
        self.backup_dir = os.path.join(self.local_backup_path, str(datetime.datetime.now().strftime('%Y%m%d%H%M%S')))
        os.makedirs(self.backup_dir)
        self.backup_file = self.backup_dir + '.tar.gz'

    def test(self):
        dump_file = os.path.join(self.backup_dir, 'database.sql')
        os.system('touch {}'.format(dump_file))

    def structure_backup(self, backup_dir=None):
        """利用mysqldump备份表结构"""
        if not backup_dir:
            backup_dir = self.backup_dir

        dump_file = os.path.join(backup_dir, 'database.sql')
        result = os.system('{} -u{} -p{} -h {} -P {} --all-databases -d --set-gtid-purged=OFF> {}'.format(
            self.mysqldump_path,
            self.mysql_user,
            self.mysql_password,
            self.mysql_host,
            self.mysql_port,
            dump_file
        ))
        if result != 0:
            logging.error('structure_backup备份失败')
            return False
        else:
            return True

    def full_backup(self, backup_dir):
        """利用percona工具整库备份"""
        if not backup_dir:
            backup_dir = self.backup_dir

        dump_dir = os.path.join(backup_dir, 'databases')
        result = os.system('{} --defaults-file={} --host={} --port={} --user={} --password={} --no-timestamp {}'.format(
            self.innobackupex_path,
            self.my_cnf,
            self.mysql_host,
            self.mysql_port,
            self.mysql_user,
            self.mysql_password,
            dump_dir
        ))
        if result != 0:
            logging.error('full_backup备份失败')
            return False
        else:
            return True

    def compress(self, source_dir=None, target_file=None):
        if not source_dir:
            source_dir = self.backup_dir
        if not target_file:
            target_file = self.backup_file
        """压缩备份文件夹"""
        try:
            with tarfile.open(target_file, "w:gz") as tar:
                tar.add(source_dir, arcname=os.path.basename(source_dir))
            shutil.rmtree(source_dir)
            return target_file
        except Exception as e:
            logging.error('压缩备份文件夹失败:{}'.format(e))
            return None

    def local_clean(self, backup_path=None, reserve_days=None):
        """清理本地历史备份"""
        if not backup_path:
            backup_path = self.local_backup_path
        if not reserve_days:
            reserve_days = self.local_reserve_days
        clean_file_list = []
        now = time.time()
        reserve_min = reserve_days * 24 * 60
        for file_name in os.listdir(backup_path):
            if file_name.endswith('.tar.gz'):
                file_path_abs = os.path.join(backup_path, file_name)
                exist_min = int((now - os.path.getmtime(file_path_abs)) / 60)
                if exist_min > reserve_min:
                    try:
                        os.remove(file_path_abs)
                    except Exception as e:
                        logging.error('删除本地备份文件失败:{}'.format(e))
                        return None
                    else:
                        logging.info('已删除过期备份文件:{}'.format(file_name))
                        clean_file_list.append(file_name)
        return clean_file_list

    def remote_backup_and_clean(self, backup_file=None, remote_path=None, reserve_days=None):
        """异地备份"""
        if not self.is_remote_backup:
            return None
        if not backup_file:
            backup_file = self.backup_file
        if not remote_path:
            remote_path = self.remote_backup_path
        if not reserve_days:
            reserve_days = self.remote_reserve_days

        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(self.ssh_host, self.ssh_port, self.ssh_user, self.ssh_password)
            sftp = paramiko.SFTPClient.from_transport(ssh.get_transport())
        except Exception as e:
            logging.error('ssh连接远程服务器失败:{}'.format(e))
            return None
        try:
            sftp.chdir(remote_path)
        except IOError:
            try:
                sftp.mkdir(remote_path)
            except Exception as e:
                logging.error('无法创建远程备份目录:{}'.format(e))
                ssh.close()
                return None
            else:
                logging.info('创建远程备份文件夹:{}'.format(remote_path))
        except Exception as e:
            logging.error('ssh未知异常:{}'.format(e))
            ssh.close()
            return None

        cmd = 'rsync -ztopg -e "ssh -o PubkeyAuthentication=yes \
-o stricthostkeychecking=no -p {}" {} {}@{}:{}/'.format(
            self.ssh_port,
            backup_file,
            self.ssh_user,
            self.ssh_host,
            remote_path
        )
        child = pexpect.spawn(cmd, [], 86400)
        try:
            while True:
                i = child.expect(['assword:', pexpect.EOF])
                if i == 0:
                    child.sendline(self.ssh_password)
                    continue
                elif i == 1:
                    child.expect(pexpect.EOF)
                    child.close()
        except pexpect.EOF:
            child.close()
            ssh.close()
            logging.error('rsync远程备份失败')
            return None

        # 清理远程备份文件夹
        now = time.time()
        reserve_min = reserve_days * 24 * 60
        files_attr = sftp.listdir_attr(remote_path)
        clean_file_list = []
        for file_attr in files_attr:
            remote_file_abs = os.path.join(remote_path, file_attr.filename)
            exist_min = int((now - file_attr.st_mtime) / 60)
            if exist_min > reserve_min:
                try:
                    sftp.remove(remote_file_abs)
                except Exception as e:
                    logging.error('删除远程备份文件失败:{}'.format(e))
                else:
                    logging.info('已删除远程过期备份文件:{}'.format(remote_file_abs))
                    clean_file_list.append(file_attr.filename)
        return clean_file_list

    def run(self):
        data = {'message': ''}
        result = self.structure_backup()
        if not result:
            data['result'] = False
            data['message'] += 'structure_backup失败\n'
            return data
        result = self.full_backup()
        if not result:
            data['result'] = False
            data['message'] += 'full_backup失败\n'
            return data
        file = self.compress()
        if file is None:
            data['result'] = False
            data['message'] += '压缩文件夹失败\n'
            return data
        file_list = self.local_clean()
        if file_list is None:
            data['result'] = False
            data['message'] += '删除本地过期备份文件失败\n'
            return data
        else:
            data['message'] += '删除本地备份文件:\n'
            for file in file_list:
                data['message'] = data['message'] + file + '\n'
        file_list = self.remote_backup_and_clean()
        if file_list is None:
            data['result'] = False
            data['message'] += '删除远程过期备份文件失败\n'
            return data
        else:
            data['message'] += '删除远程备份文件:\n'
            for file in file_list:
                data['message'] = data['message'] + file + '\n'
        data['result'] = True
        return data


if __name__ == '__main__':
    mb = MysqlBackup()
    mb.run()
