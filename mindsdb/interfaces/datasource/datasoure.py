import os
import shutil
import venv
import ray

from helpers.hash import md5_dir, md5_file


class DataSource:

    def __init__(self, module_name :str) -> None:

        self.base_path = os.path.abspath("../../datasources/")
        self.module_name = module_name
        self.module_name = self.module_name
        self.module_path = os.path.abspath("{base_path}/{module_mame}".format(module_mame=module_name, base_path=self.base_path))
        self.module_venv_path = os.path.abspath('{base_path}/{module_mame}/.venv'.format(module_mame=module_name, base_path=self.base_path))
        
        # check for module integrity
        if not os.path.isdir(self.module_path):
            raise Exception('Module path does not exist')
        
        if not os.path.isfile("{path}/requirements.txt".format(path=self.module_path)):
            raise Exception('Module needs a requirements.txt file')

        if not os.path.isfile("{path}/{module}_ds.py".format(path=self.module_path, module=self.module_name)):
            raise Exception('Module needs a {module}_ds.py file'.format(module=self.module_name))

        if not os.path.isdir("{path}/.venv".format(path=self.module_path)):
            os.mkdir("{path}/.venv".format(path=self.module_path))

        # if everything checks, create a folder name with a unique hash for the module requirements.txt in {module}/.venv
        self.hash = md5_file('{path}/requirements.txt'.format(path=self.module_path)) #, ignore=['.venv', '{module}_ds.py'.format(module=self.module_name)])
        self.hash_path = os.path.abspath('{base_path}/{module_mame}/.venv/{hash}'.format(
            module_mame=module_name,
            hash=self.hash, 
            base_path=self.base_path))
        # create venv
        self.create_venv()
        
    
    """
    Launch cluster
    """
    def launch_cluster(self, port):
        
        os.system("{hash_path}/bin/python {module_path}/{module}_ds.py --action start --port {port}".format(
            module_path=self.module_path, 
            hash_path=self.hash_path, 
            module=self.module_name,
            port=port))
    

    """
    Creates a virtual environment for a module
    """
    def create_venv(self, overwrite = False) -> bool:
        

        dir_exists = os.path.isdir(self.module_venv_path)
        hash_exists = os.path.isdir(self.hash_path) 
        
        if hash_exists and overwrite == False:
            return False

        # if the venv folder hasnt been created or the hash doesnt match, create it
        if not hash_exists or overwrite is True:
            if dir_exists:
                shutil.rmtree(self.module_venv_path)
            os.mkdir(self.module_venv_path)

        # Create virtual enviornment
        venv.create(self.hash_path, with_pip=True)
        # install ray
        os.system("{hash_path}/bin/pip3 install ray".format(hash_path=self.hash_path))
        # install the requirements.txt
        os.system("{hash_path}/bin/pip3 install -r {path}/requirements.txt".format(hash_path=self.hash_path, path=self.module_path))
        
        return True

#    

ds= DataSource('snowflake')
ds.launch_cluster(port= 61200)

# actor = ray.get_actor("SnowflakeDS")
# b = actor.connect.remote()
# print(b)

#ray.shutdown()    




