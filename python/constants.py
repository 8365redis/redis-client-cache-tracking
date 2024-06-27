
#KEEP SYNC WITH src/constants.h

CCT_MODULE_PREFIX = "CCT:"
CCT_K2C  = CCT_MODULE_PREFIX + "K2C:"
CCT_Q2C = CCT_MODULE_PREFIX + "Q2C:"
CCT_C2Q = CCT_MODULE_PREFIX + "C2Q:"
CCT_K2Q  = CCT_MODULE_PREFIX + "K2Q:"
CCT_Q2K  = CCT_MODULE_PREFIX + "Q2K:"
CCT_QC =  CCT_MODULE_PREFIX + "QC:" 
CCT_DELI =  ":" 
CCT_Q_DELI =  "-CCT_DEL-"
CCT_EOS =  "-END_OF_SNAPSHOT-"

CCT_OPERATION = "operation" 
CCT_KEY = "key" 
CCT_VALUE = "value" 
CCT_QUERIES = "queries" 


CCT_QUERY_TTL = 4  # This must be same as CCT_QUERY_TTL_SECOND in constants.h with _DEBUG
CCT_QUERY_HALF_TTL = ( CCT_QUERY_TTL / 2 ) + 1

CCT_HEART_BEAT_INTERVAL = 2 # This must be same as CCT_CLIENT_TTL_CHECK_INTERVAL_SECOND in constants.h with _DEBUG

CCT_NOT_REGISTERED_COMMAND_ERROR  = "Not registered client"

SKIP_HB_TEST = False
SKIP_PERF_TEST = True
