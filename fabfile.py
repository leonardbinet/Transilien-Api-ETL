from fabric.contrib.files import append, exists, sed
from fabric.api import env, local, run, put, sudo
import random
import os
from os import path
import glob
from fabric.api import *

# NEEDS TO BE CONFIGURED
env.key_filename = "~/.ssh/aws-eb2"

REPO_URL = 'https://github.com/leonardbinet/Transilien-Api-Scraper.git'
PROJECT_NAME = "api_transilien"
SECRET_PATH = "secret.json"


# ONLY NECESSARY IF YOU WANT TO USE AWS TASKRUNNER
TASKRUNNER_CRED_PATH = "taskrunner_credentials.json"
S3_TASKRUNNER_URL = "https://s3.amazonaws.com/datapipeline-us-east-1/us-east-1/software/latest/TaskRunner/TaskRunner-1.0.jar"
S3_MYSQL_CONNECTOR_URL = "http://s3.amazonaws.com/datapipeline-prod-us-east-1/software/latest/TaskRunner/mysql-connector-java-bin.jar"
AWS_WORKERGROUP = "SNCF_TASKS"
AWS_REGION = "eu-west-1"
S3_LOG_URI = "s3://pipelinetrain/taskrunner"


# do not change
site_folder = '~/tasks/%s' % (PROJECT_NAME)
source_folder = site_folder + '/source'
remote_tr_cred_path = os.path.join(source_folder, TASKRUNNER_CRED_PATH)
remote_tr_path = os.path.join(site_folder, "taskrunner", "TaskRunner-1.0.jar")


def deploy():
    _create_directory_structure_if_necessary(site_folder)
    _get_latest_source(source_folder)
    _send_secret_jsons()
    _update_virtualenv(source_folder)
    # _send_cron_tasks()


def initial_deploy():
    _set_environment()
    deploy()


def deploy_taskrunner():
    _install_java()
    _download_taskrunner()


def _send_secret_jsons():
    put(TASKRUNNER_CRED_PATH, os.path.join(source_folder, TASKRUNNER_CRED_PATH))
    put(SECRET_PATH, os.path.join(source_folder, SECRET_PATH))


def _create_directory_structure_if_necessary(site_folder):
    for subfolder in ('virtualenv', 'source', 'logs', 'taskrunner'):
        run('mkdir -p %s/%s' % (site_folder, subfolder))


def _get_latest_source(source_folder):
    if exists(source_folder + '/.git'):
        run('cd %s && git fetch' % (source_folder))
    else:
        run('git clone %s %s' % (REPO_URL, source_folder))
    current_commit = local("git log -n 1 --format=%H", capture=True)
    run('cd %s && git reset --hard %s' % (source_folder, current_commit))


def _set_environment():
    sudo('apt-get update')
    sudo('apt-get install python3-pip python3-dev libpq-dev')
    sudo('pip3 install virtualenv')


def _install_java():
    sudo('add-apt-repository -y ppa:webupd8team/java')
    sudo('apt-get update')
    sudo("sh -c 'echo debconf shared/accepted-oracle-license-v1-1 select true | debconf-set-selections'")
    sudo("sh -c 'echo debconf shared/accepted-oracle-license-v1-1 seen true | debconf-set-selections'")
    sudo('apt-get -y install oracle-java8-installer')


def _update_virtualenv(source_folder):
    virtualenv_folder = source_folder + '/../virtualenv'
    if not exists(virtualenv_folder + '/bin/pip'):
        run('virtualenv --python=python3 %s' % (virtualenv_folder,))
    run('%s/bin/pip install -r %s/requirements.txt' % (
        virtualenv_folder, source_folder
    ))


def _download_taskrunner():
    taskrunner_folder = path.join(site_folder, "taskrunner")
    run("wget -P " + taskrunner_folder + " " + S3_TASKRUNNER_URL)
    run("wget -P " + taskrunner_folder + " " + S3_MYSQL_CONNECTOR_URL)


def _send_cron_tasks():
    loc_cron_files = glob.glob("cron/")
    for loc_cron_file in loc_cron_files:
        rem_cron_fil = os.path.join(
            "/etc/cron.d", os.path.basename(loc_cron_file))
        put(loc_cron_file, rem_cron_fil, use_sudo=True)
        sudo("chmod +rx %s" % rem_cron_fil)
        sudo("chown root:root %s" % rem_cron_fil)


def start_taskrunner():

    run("java -jar %s --config %s --workerGroup=%s --region=%s --logUri=%s" %
        (remote_tr_path, remote_tr_cred_path, AWS_WORKERGROUP, AWS_REGION, S3_LOG_URI))


##### CRON #####

def _marker(marker):
    return ' # MARKER:%s' % marker if marker else ''


def _get_current():
    with settings(hide('warnings', 'stdout'), warn_only=True):
        output = run('crontab -l')
        return output if output.succeeded else ''


def crontab_set(content):
    """ Sets crontab content """
    run("echo '%s'|crontab -" % content)


def crontab_show():
    """ Shows current crontab """
    puts(_get_current())


def crontab_add(content, marker=None):
    """ Adds line to crontab. Line can be appended with special marker
    comment so it'll be possible to reliably remove or update it later. """
    old_crontab = _get_current()
    crontab_set(old_crontab + '\n' + content + _marker(marker))


def crontab_remove(marker):
    """ Removes a line added and marked using crontab_add. """
    lines = [line for line in _get_current().splitlines()
             if line and not line.endswith(_marker(marker))]
    crontab_set("\n".join(lines))


def crontab_update(content, marker):
    """ Adds or updates a line in crontab. """
    crontab_remove(marker)
    crontab_add(content, marker)
