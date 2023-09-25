
#KEEP SYNC WITH src/constants.h

CCT_MODULE_PREFIX = "CCT:"
CCT_K2C  = CCT_MODULE_PREFIX + "K2C:"
CCT_Q2C = CCT_MODULE_PREFIX + "Q2C:"
CCT_C2Q = CCT_MODULE_PREFIX + "C2Q:"
CCT_K2Q  = CCT_MODULE_PREFIX + "K2Q:"
CCT_Q2K  = CCT_MODULE_PREFIX + "Q2K:"
CCT_QC =  CCT_MODULE_PREFIX + "QC:" 
CCT_DELI =  ":" 
CCT_Q_DELI =  chr(5) 

CCT_OPERATION = "operation" 
CCT_KEY = "key" 
CCT_VALUE = "value" 
CCT_QUERIES = "queries" 


CCT_TTL = 4  # This must be same as CCT_TTL in constants.h
CCT_HALF_TTL = ( CCT_TTL / 2 ) + 1