__author__ = 'Qubole-SB'
from optparse import OptionParser
import requests,json,re,sys,os,io
import pandas as pd
from pandas.io.json import json_normalize
import datetime,math 


class fileHandle:
    def __init__(self,filename):
        self.filename = filename
    def openFile(self):
        if os.path.exists(self.filename):
            self.file = io.open(self.filename, 'a',encoding="utf-8")
        else:
            self.file = io.open(self.filename, 'w',encoding="utf-8")
    def writeFile(self,msg):
        if sys.version_info[0] > 2:
            self.file.write(msg)
        else:
            self.file.write(unicode(msg))    
    def closeFile(self):
        self.file.close()


# Perform rest call 
def apiCall ( operation, per_page=None, page=None ):
    env = opts.Env
    v2List = ['azure','oraclecloud']
    api_version = 'v1.2'
    if operation == 'clusters/':
        if env in v2List:
            api_version = 'v2'  
        else:
            api_version = 'v1.3'    
    data = {'env': env, 'operation': operation, 'page' : page , 'per_page' : per_page , 'api_version' : api_version}
    if page != None:
        if '?' in operation:
            url = 'https://%(env)s.qubole.com/api/%(api_version)s/%(operation)s&per_page=%(per_page)s&page=%(page)s' % data
        else:
            url = 'https://%(env)s.qubole.com/api/%(api_version)s/%(operation)s?per_page=%(per_page)s&page=%(page)s' % data
    else:
        url = 'https://%(env)s.qubole.com/api/%(api_version)s/%(operation)s' % data    
    if operation != 'account/':
        log('\nCall URL - ' + url)
    headers = {"X-AUTH-TOKEN" : opts.Token, "Content-Type" : "application/json" , "Accept" : "application/json"}
    request = requests.get(url=url,headers=headers) 
    rjson = request.json()
    if rjson == None or rjson == "":
        rjson = json.loads('{}')
    return rjson


# get info for all notebooks
def getNotebooks(folderType):
    rjson = apiCall(operation='notebooks/search.json?location=%s'%folderType)     
    if rjson.get('notes') != []:
        nb = json_normalize(rjson,'notes').sort_values(by=['qbol_user_id','location','name'])
        return nb
    else:
        return pd.DataFrame({'dummyColumn' : []}) 


# export notebook
def exportNotebooks(notebookid):
    log('\nExporting Notebook ID - ' + str(notebookid))
    rjson = apiCall(operation='notebooks/%s/export'%notebookid) 
    if 'error' in rjson or 'error_message' in rjson:
        log('\nError - '+json.dumps(rjson))
        log('\nExport failed')
        return {}
    else:
        log('\nExport completed')
        return rjson

            
# log successful searched notebook info to csv
def updateSearchedNotebooks(notebooktype,nbname,notebookid,clusterID,clusterLabel,location,notebookowner,parano,paratitle,lineNos,notebookurl):
    data = {  'notebooktype': notebooktype
            , 'nbname': nbname 
            , 'notebookid' : str(notebookid)
            , 'clusterID' : str(clusterID)
            , 'clusterLabel' : clusterLabel
            , 'location' : location
            , 'notebookowner' : notebookowner
            , 'parano' : str(parano)
            , 'paratitle' :paratitle
            , 'lineNos' : lineNos
            , 'notebookurl' : notebookurl
           }
    #searchResultFile.writeFile("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n"%(notebooktype,nbname,str(notebookid),str(clusterID),clusterLabel,location,notebookowner,str(parano),paratitle,lineNos,notebookurl))
    searchResultFile.writeFile('"%(notebooktype)s","%(nbname)s",%(notebookid)s,%(clusterID)s,"%(clusterLabel)s","%(location)s","%(notebookowner)s",%(parano)s,"%(paratitle)s","%(lineNos)s","%(notebookurl)s"\n'%(data))


def log(msg):
    logFile.writeFile(msg)    


# get cluster labels to map notebooks to cluster id
def getClusterLabels(per_page,page):
    v2List = ['azure','oraclecloud']
    labeldict = {}
    rjson = apiCall(operation='clusters/',page=page,per_page=per_page)
    if rjson.get('clusters') != []:
        nb = json_normalize(rjson['clusters'])
        if opts.Env in v2List:
            labeldict = dict(zip( nb['id'], nb['cluster_info.label'].apply(lambda x: '|'.join(x)) ))
        else:
            labeldict = dict(zip( nb['id'], nb['label'].apply(lambda x: '|'.join(x)) ))    
        nextpage = rjson['paging_info']['next_page']
        if nextpage != None :
            nextdict = getClusterLabels(per_page=per_page,page=nextpage)
            labeldict.update(nextdict)         
    return labeldict


# get account info 
def getAccountInfo():
    rjson = apiCall(operation='account/') 
    acc = json_normalize(rjson)
    return acc


# get account users
def getUsers():
    log('\n\nGet user info')
    rjson = apiCall(operation='accounts/get_users/') 
    usrs = json_normalize(rjson,'users')
    emaildict = dict(zip( usrs['id'] , usrs['email'])) 
    userdict = dict(zip( usrs['email'], usrs['id'] ))
    log('\nUser Dictionary - \n')
    log(str(userdict))  
    log('\nEmail Dictionary - \n')
    log(str(emaildict))
    return userdict,emaildict


#check users
def checkUsers():
    userIDsFilterList = []
    emailFnd = False
    log('\n\nValidate input users in the account')
    for emailID in opts.Users:
        userID = users.get(emailID)
        if userID is None:
            log('\nUser ' + emailID + ' not found in account')
        else:
            userIDsFilterList.append(userID)
            emailFnd = True  
    if not emailFnd:
        log('\nNo user email(s) ' + ','.join(opts.Users) + ' found in the account')
        log("\n\nExiting Program\n")
        exit(1)
    log('\nFinished Validating input users in the account')    
    return userIDsFilterList    


#find pattern
def findPattern(nbcontent):
    paraDTL = {}
    if 'paragraphs' in nbcontent:
      for i in range (0, len (nbcontent['paragraphs'])):
        paraLogged = False
        if 'title' in nbcontent['paragraphs'][i] :
            title = nbcontent['paragraphs'][i]['title']
        else:
            title = ''
        para_no = i + 1    
        if 'text' in nbcontent['paragraphs'][i] :
            lineList = []
            srchPatternList = opts.SrchPattern.split('~')
            if sys.version_info[0] > 2:
                content = nbcontent['paragraphs'][i]['text']
            else:
                content = nbcontent['paragraphs'][i]['text'].encode('utf8')
            for srchPattern in srchPatternList:
                if opts.CaseSensitiveSrch == 'N':
                    p = re.compile(srchPattern,re.I|re.M|re.DOTALL)
                else:
                    p = re.compile(srchPattern,re.M|re.DOTALL)
                lineNo = 1    
                for line in content.splitlines():
                    matchObj = p.search(line)
                    if matchObj:
                        if not paraLogged:
                            paraLogged = True
                            log('\nFound match in Paragraph - %s'%(str(para_no)))
                            if title != '':
                                log('\nWith Paragraph Title - %s'%(title))
                        log("\nMatch on line No " + str(lineNo) + " for pattern " + srchPattern )
                        log("\n****")
                        log("\n"+ line)
                        log("\n****")
                        paraMatchFnd = True
                        lineList.append(lineNo)
                    lineNo = lineNo + 1
            if paraLogged:
                lineList = sorted(list(set(lineList)))
                paraDTL[para_no] =  { "title" : title , "lineNos" : '|'.join(map(str,lineList)) }
    return paraDTL           


# main logic
def searchNotebooks(folderType):
    log('\n\n--- Starting Search for %s Notebooks ---' % folderType)
    log('\n\nGet %s Notebooks' %folderType)
    nb = getNotebooks(folderType)
    summaryList.append('\n\n****** Summary for %s Notebooks ******' % folderType) 
    summaryList.append('\nTotal Notebooks - %s' % str(len(nb.index)))   
    #Filter notebooks for users
    if len(opts.Users) > 0 and len(nb.index) > 0:
        summaryList.append('\nNotebooks skipped due to user filter - %s' % str(len(nb.index)-len(nb[nb.qbol_user_id.isin(userIDsFilterList)].index)))
        nb = nb[nb.qbol_user_id.isin(userIDsFilterList)]
    if len(nb.index) > 0:
        log('\n\n--- Starting Search Process for %s Notebooks ---' % folderType)
        nbsuccesscnt = 0
        nbfailcnt = 0
        nbaccesscnt = 0
        for index, row in nb.iterrows():
            notebooktype = row['note_type']
            nbname = row['name']
            notebookid = row['id']
            location = row['location']
            notebookowner = emails.get(row['qbol_user_id'])
            log('\n\nNotebook Name - ' + nbname)
            notebookurl = "https://" + opts.Env + ".qubole.com/notebooks#" + folderType.lower() + "?id=" + str(notebookid)
            log("\nNotebook URL - " + notebookurl)
            if math.isnan(row['cluster_id']) or  clusterLabels.get(int(row['cluster_id'])) == None:
                clusterLabel = ""
                clusterID = ""
                log('\nNo cluster attached to notebook')
            else:
                clusterID = int(row['cluster_id'])
                clusterLabel = clusterLabels.get(clusterID)       
            nbcontent = exportNotebooks(notebookid)          
            if len(nbcontent)>0:
                log('\nSearching pattern for notebook')    
                paraDTLS = findPattern(nbcontent)
                log('\nPattern search finished for notebook') 
                if len(paraDTLS)>0:
                    nbsuccesscnt = nbsuccesscnt + 1
                    log('\nWriting to csv')
                    paraNoList = sorted(paraDTLS.keys())
                    for para_no in paraNoList:
                        updateSearchedNotebooks(notebooktype,nbname,notebookid,clusterID,clusterLabel,location,notebookowner,para_no,paraDTLS[para_no]["title"],paraDTLS[para_no]["lineNos"],notebookurl)
                else:
                    nbfailcnt = nbfailcnt + 1
            else:
                nbaccesscnt = nbaccesscnt + 1                
        log('\n\n--- Searching for %s Notebooks Completed ---' % folderType)
        summaryList.append('\nNotebooks skipped due to access issues - %s' % str(nbaccesscnt))
        summaryList.append('\nNotebooks with no pattern match - %s' % str(nbfailcnt))
        summaryList.append('\nNotebooks with successful pattern match - %s' % str(nbsuccesscnt))   
    else:
        log('\n\n--- No %s Notebooks to search ---' % folderType)            
          

def argumentExit(optparser):
    optparser.print_help()
    exit(1)    


def checkArguments():
    ynOptions = ['y','n','Y','N']
    optparser = OptionParser()
    optparser.add_option("-e", "--Env", dest="Env" , default="api" , help="Mandatory - provide the Qubole enviornment, default value is api")
    optparser.add_option("-t", "--Token", dest="Token", default="", help="Mandatory - provide your Qubole account token")
    optparser.add_option("-p", "--SearchPattern", dest="SrchPattern", default="", help="Mandatory - search multiple string or pattern seperated by ~ character, case insensitive search by default")
    optparser.add_option("-u", "--Users", dest="Users", default="", help="Optional - comma seperated list of email ids")
    optparser.add_option("-s", "--CaseSensitiveSearch", dest="CaseSensitiveSrch", default="N", choices=ynOptions , help="Optional - case sensitive search , default value is N")
    optparser.add_option("-c", "--CommonDirectory", dest="CommonDirectory", default="Y", choices=ynOptions , help="Optional - include common directory , default value is Y")
    (opts, args) = optparser.parse_args()
    opts.Env = opts.Env.lower().replace(" ","")
    if opts.Env == "":
        print("\nPlease specify an enviornment.")
    opts.CaseSensitiveSrch = opts.CaseSensitiveSrch.upper()
    opts.CommonDirectory = opts.CommonDirectory.upper()
    if (opts.Token == None or opts.Token == '' or opts.SrchPattern == None or opts.SrchPattern == ''):
        argumentExit(optparser)
    elif (len(opts.Users) > 0):
        # remove spaces and create a list
        userList = opts.Users.replace(" ","").split(",")
        #remove dups
        userList = list(set(userList))
        #remove empty
        userList = ''.join(userList).split()
        if (len(userList) > 0 ):
            p = re.compile(r"[^@]+@[^@]+\.[^@]+")
            for user in userList:
                if not p.match(user):
                    print("\nInvalid email id -  %s \n" %user)
                    argumentExit(optparser)
        else:
            print("\nEmail ids cannot be empty strings.\n")
            argumentExit(optparser)
        opts.Users = userList
        return (opts,args)
    else:
        return (opts,args)    


#### Start main ###
summaryList = []
now = datetime.datetime.now()
nowStr = now.strftime("%Y%m%d%H%M%S")
(opts, args) = checkArguments()

# Generate log file name using account id
acc = getAccountInfo()
searchResultFileName = 'NotebookSearchResult_' + nowStr  + '.csv'
logFileName =  'NotebookSearchResult_'+ nowStr + '.log'
logFile = fileHandle(logFileName)
logFile.openFile()
log('--- Execution Started ---')
log('\nStarting Execution Time - %s' % str(now))
log('\n\nEnviornment - %s' %opts.Env)
log('\nAccount Name - %s' % acc['name'][0])
log('\nAccount ID - %s' % acc['id'][0])
log('\n\nSearch Results CSV - %s' % searchResultFileName)

# get cluster labels and users 
log('\n\nGet cluster labels')
clusterLabels = getClusterLabels(per_page=100,page=1)
if len(clusterLabels) == 0:
    log('\nNo Clusters found in the account')
else:
    log('\nCluster Labels -\n')
    log(str(clusterLabels))    
(users,emails) = getUsers()

#checkUsers -
userIDsFilterList = []
if len(opts.Users) > 0: 
    userIDsFilterList = checkUsers()
        
# Track copied notebooks in a csv file , on rerun already copied notebooks will be skipped
searchResultFile = fileHandle(searchResultFileName)
searchResultFile.openFile()
searchResultFile.writeFile(u"notebooktype,notebookname,notebookid,clusterid,clusterlabels,notebookdirectory,notebookowner,paragraphposition,paragraphtitle,linenos,notebookurl\n")

#Search User Notebooks
searchNotebooks('Users')

#Search Common Notebooks
if (opts.CommonDirectory == 'Y'):
    searchNotebooks('Common')
else:
    log('\n--- Skipping Common Directory  ---')

for line in summaryList:
    log(line)    

searchResultFile.closeFile()    
now = datetime.datetime.now()
log('\n\nEnding Execution Time - %s' % str(now))
log('\n--- Execution Completed ---')
logFile.closeFile()

