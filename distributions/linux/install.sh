#!/bin/bash
set -e
set -o pipefail

cmdcol="$(tput sgr0)$(tput bold)"
normalcol="$(tput sgr0)"
trap 'echo -n "$normalcol"' DEBUG

echo -e """
\e[38;5;35m
          ____________________
        /░                    ---------________
      /░                                       --_
    /░                                            --_
   /░                                                --_
  /░                                                    --_
 /░                                         __--__         --_
|░                                      __--       ---___      _
|░                                     -       /|      ---___-
|░         _________________          |      /░  |
|░        /              |░           |     /░    |
|░       /                |░         |     /░      |
 |░     /       / \        |░        |     \░      |
 |░    /       /░   \       |░      |       \░     |
 |░    /        \░   \       |░     |        \░    |
 |░___/          \░___\       |░___|          \░___|

           █▀▄▀█ ░▀░ █▀▀▄ █▀▀▄ █▀▀ █▀▀▄ █▀▀▄
           █░▀░█ ▀█▀ █░░█ █░░█ ▀▀█ █░░█ █▀▀▄
           ▀░░░▀ ▀▀▀ ▀░░▀ ▀▀▀░ ▀▀▀ ▀▀▀░ ▀▀▀░
$cmdcol


"""

# Attempt to detect python
python_path="$(which python3)"
pip_path="$(which pip3)"
printf "Detected python3: $python_path\ndetected pip3: $pip_path\nDo you want to use these [Y] or manually provide paths [N]? [Y/N]"
read use_detected_python
if [ "$use_detected_python" = "N" ] || [ "$use_detected_python" = "n" ]; then
    echo "Please enter the path to your python (3.6+) interpreter:"
    read python_path

    echo "Please enter the path to your associate pip installation:"
    read pip_path
fi
export MDB_INSTALL_PYTHONPATH="$python_path"
export MDB_INSTALL_PIPPATH="$pip_path"

# Check that it's indeed python 3.6 and that pip works
${python_path} -c "import sys; print('Sorry, MindsDB requires Python 3.6+') and exit(1) if sys.version_info < (3,6) else exit(0)"
${pip_path} --version > /dev/null 2>&1

echo "Do you want us to install using the default parameters [Y] or should we go ahead and give you control to tweak stuff during installation ? [Y/N]"
read default_install
export MDB_DEFAULT_INSTALL="$default_install"

export MDB_MAKE_EXEC="Y"
if [ "$EUID" -ne 0 ]; then
  install_as="user"
else
  install_as="global"
fi

if [ "$default_install" = "N" ] || [ "$default_install" = "n" ]; then
  if [ "$EUID" -ne 0 ]; then
      install_as="user"
      echo "You are currently installing Mindsdb for your user only, rather than globally. Is this intended ? [Y/N]"
      read approve
      if [ "$approve" = "N" ] || [ "$approve" = "n" ]; then
          echo "Please run the installer using sudo in front of the command"
          exit
      fi
    else
      install_as="global"
      echo "You are currently installing Mindsdb globally (as root), is this intended ? [Y/N]"
      read approve
      if [ "$approve" = "N" ] || [ "$approve" = "n" ]; then
          echo "Please run the installer as your desired user instead (without using sudo in front of it)"
          exit
      fi
  fi

  echo "Should we make an executable for mindsdb (in /usr/bin/ if installing as root or in your home directory if install as user)? [Y/N]"
  read make_exec
  export MDB_MAKE_EXEC="$make_exec"
fi


cmdcol="$(tput sgr0)$(tput bold)"
normalcol="$(tput sgr0)"
trap 'echo -n "$normalcol"' DEBUG

echo -e """
This might take a few minutes (dozens of minutes ?, no longer than half an hour, pinky promise).
Go grab a coffee or something and wait for the inevitable error log 99% of the way through

\e[38;5;35m

_,-||*||-~*)
(*~_=========\

|---,___.-.__,\

|        o     \ ___  _,,,,_     _.--.
\      -^-    /*_.-|~      *~-;*     \

 \_      _  ..                 *,     |
   |*-                           \.__/
  /                      ,_       \  *.-.
 /    .-~~~~--.            *|-,   ;_    /
|              \               \  | ****
 \__.--.*~-.   /_               |.
            ***  *~~~---..,     |
                         \ _.-.*-.
                            \       \

                             ..     /
                               *****
$cmdcol

"""

# Python code below
cat << EOF > _install.py
#!$python_path

import sys
import os
from pathlib import Path
import time
from os.path import expanduser


install_as  = sys.argv[1]
python_path = sys.argv[2]
pip_path    = sys.argv[3]
default_install = sys.argv[4]
make_exec = sys.argv[5]
home = expanduser("~")
mdb_home = os.path.join(home, 'mindsdb')

default_install = False if default_install.lower() == 'n' else True
make_exec = False if make_exec.lower() == 'n' else True


if install_as == 'user':
    config_dir = os.path.join(mdb_home,'data', 'config')
    storage_dir = os.path.join(mdb_home,'data', 'storage')
else:
    config_dir = os.path.join('/etc/mindsdb/')
    storage_dir = os.path.join('/var/lib/mindsdb/')

os.makedirs(config_dir,exist_ok=True)
os.makedirs(storage_dir,exist_ok=True)

print(f'Configuration files will be stored in {config_dir}')
print(f'Datasources and predictors will be stored in {storage_dir}')

datasource_dir = os.path.join(storage_dir,'datasources')
predictor_dir = os.path.join(storage_dir,'predictors')

os.makedirs(datasource_dir,exist_ok=True)
os.makedirs(predictor_dir,exist_ok=True)

print(f'\nInstalling some large dependencies via pip ({pip_path}), this might take a while\n')
time.sleep(3)

# Consider adding:  --force-reinstall
# How it installs itself (maybe instead of github just use local download if user has cloned everything ?)
if install_as == 'user':
    os.system(f'{pip_path} install  git+https://github.com/mindsdb/mindsdb_server.git@split --upgrade')
else:
    os.system(f'sudo {pip_path} install git+https://github.com/mindsdb/mindsdb_server.git@split --upgrade')

dataskillet_source = None
lightwood_source = f'git+https://github.com/mindsdb/lightwood.git@stable'
mindsdb_native_source = f'git+https://github.com/mindsdb/mindsdb_native.git@stable'

for source in [dataskillet_source,lightwood_source,mindsdb_native_source]:
    if isinstance(source,str):
        if install_as == 'user':
            os.system(f'{pip_path} install {source} --upgrade')
        else:
            os.system(f'sudo {pip_path} install {source} --upgrade')
time.sleep(1)
print('Done installing dependencies')
print('\nLast step: Configure Mindsdb\n')

from mindsdb_server.utilities.wizards import cli_config,daemon_creator,make_executable
config_path = cli_config(python_path,pip_path,predictor_dir,datasource_dir,config_dir,use_default=default_install)

if install_as == 'user':
    pass
else:
    daemon_creator(python_path,config_path)

if make_exec:
    if install_as == 'user':
        path = str(os.path.join(mdb_home,'run'))
    else:
        path = '/usr/bin/mindsdb'

    make_executable(python_path,config_path,path)

print('Installation complete !')

if make_exec:
    print(f'You can use Mindsdb by running {path}. Or by importing it as the mindsdb library from within python.')
else:
    print(f'You can use Mindsdb by running {python_path} -m mindsdb_server --config={config_path}. Or by importing it as the mindsdb library from within python.')


EOF
#/Python code

chmod 755 _install.py

INSTALLER_SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

"${MDB_INSTALL_PYTHONPATH}" "$INSTALLER_SCRIPT_DIR"/_install.py "$install_as" "$MDB_INSTALL_PYTHONPATH" "$MDB_INSTALL_PIPPATH" "$MDB_DEFAULT_INSTALL" "$MDB_MAKE_EXEC"

rm _install.py