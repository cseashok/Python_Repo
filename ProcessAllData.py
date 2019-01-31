import sys
import os
import sys
sys.path.append('./Imports')
import Putil
from Putil import Putil
import Compiler
from Compiler import Compiler
#from WeeklyUtilization import WeeklyUtilization

import json

def load_configuration():
    '''Simply loads the json register lookup file'''
    with open('register-lookup.json', 'r') as inputFile:
        config = json.load(inputFile)
    return config

config = load_configuration()
processor = Putil(config)
processor.run_data_import()


comp = Compiler(processor.data, config)
comp.run_compilation()


## First section looking at a week by week breakdown of wheel true utilization
#analyzer = WeeklyUtilization(processor.data)
#analyzer.run_utilization_study()
