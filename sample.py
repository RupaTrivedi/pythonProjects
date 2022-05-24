#from DbUtil import RDBMSUtil
from Customer import Customer
from FTEM_Menifest_Parser import FTEM_Parser

custObj = FTEM_Parser().getCustomer()
print("Customer name is ", custObj.name)
#db_object=RDBMSUtil()
#print("hello from sample")
#db_object.check_table_exists('FTEM_PREPROCESSED_DATA')
