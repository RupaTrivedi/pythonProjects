import string
import json
from typing import Optional
from pydantic import BaseModel

class Attribute(object):
    name : str
    default_value : str

class AssetType(object):
    name : str
    attributes : Optional[list]

class Application(object):
    name : str
    plant : str
    assetType : Optional[list]

class Tag(object):
    name : str
    sampling_rate : Optional[str]
    data_type : Optional[str]

class Program(object):
    name : str
    ingressSchema : Optional[str]
    tags : Optional[list]

class Network(object):
    name: str
    type : Optional[str]
    ipAddresses : Optional[list]
    protocol : Optional[str]

class Controller(object):
    name : str
    type : Optional[str]
    networks : Optional[list]
    programs : Optional[list]

class BrokerEndPoint(object):
    name : str

class Machine(object):
    name : str
    asset_types : Optional[list]
    controllers : Optional[list]
    broker_endpoints : Optional[list]

class Line(object):
    name : str
    machines : Optional[list]

class Area(object):
    name : str
    lines : Optional[list]

class Topic(object):
    name : str
    data_type : str

class Broker(object):
    name : str
    type : str
    ipaddress : Optional[list]
    networks : Optional[list]
    topics : Optional[list]

class Appliance(object):
    name : str
    type : str
    ipAddresses : Optional[list]
    ingress_schema : str
    networks : Optional[list]

class DataProvider(object):
    controllers : Optional[list]
    brokers : Optional[list]
    appliances : Optional[list]

class Schema(object):
    name : str
    sample_val : str
    data_format : str

class Plant(object):
    name : str
    longitude : str
    latitude : str
    area : Optional[list]
    dataProviders: Optional[list]
    networks : Optional[list]
    schemas : Optional[list]

class Customer(object):
    name: str
    applications : Optional[list]
    plants : Optional[list]
