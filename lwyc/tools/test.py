import os
import sys
import pandas as pd
from pandas import DataFrame
import numpy as np
from sklearn import linear_model
from tools.db import *
from datetime import datetime
import re


data = ['zn', 'znn', 'znnn']
print(data)
data = DataFrame(list(data))
data.columns = ['name']
data.stack()
data.unstack()