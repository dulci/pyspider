# -*- encoding: utf-8 -*-
from pyspider.libs.web_driver import Mydriver
from pyspider.libs.singleton import Singleton

class WebDrivers(metaclass=Singleton):
    def __init__(self):
        self.drivers = {}
    
    def get_driver(self, project, can_new=False, load_img=False):
        if can_new and not self.drivers.get(project):
            self.drivers[project] = Mydriver.chrome_driver(load_img)
            return self.drivers.get(project)
        return self.drivers.get(project)
    
    def destroy_driver(self, project):
        if self.drivers.get(project):
            self.drivers.get(project).quit()
            del self.drivers[project]

    def delete_driver(self, project):
        if self.drivers.get(project):
            self.drivers.pop(project)
