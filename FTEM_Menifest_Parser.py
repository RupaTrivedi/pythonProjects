import json
import jsonpickle
from collections import namedtuple
from json import JSONEncoder
from Customer import Customer

class FTEM_Parser:


    def __init__(self):
        with open("menifest\\sample_menifest.json") as f:
            data = json.load(f)
  #      str_data = json.dumps(data)
  #      x = json.loads(str_data, object_hook =
#            lambda d : namedtuple('X', d.keys())
#               (*d.values()))
        self.customerObj = self.parseToCustomer(data)

    def parseToCustomer(self, obj):
        print(obj['name'])
        cust = Customer()
        cust.name = obj.customer.name
        cust.applications = obj.customer.applications
        cust.plants = obj.customer.plants
        return cust

    def getCustomer(self):
        return self.customerObj