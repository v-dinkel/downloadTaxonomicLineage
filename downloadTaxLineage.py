# -*- coding: utf-8 -*-
"""
Created on Thu May  5 10:58:07 2022

@author: vdinkel
"""

import requests
import pandas as pd
import json

def getIDsFromKrakenReport(krakenFileDir, idColumn = ""):
    print("--getIDsFromKrakenReport")
    ids = []
    chunksize = 10 ** 5
    k = 0
    headerID = 5 # this is the default kraken column
    for chunk in pd.read_csv(krakenFileDir, chunksize=chunksize, sep="\t", header=None):
        print(k)
        if idColumn != "" and k == 0:
            headerID = list(chunk.loc[0].values).index(idColumn) # identify column index of custom column name
        
        ids.append(list(set(chunk.iloc[:, headerID].values))) 
        k+=1
    flat_list = list(set([item for sublist in ids for item in sublist]))
    flat_list.remove(idColumn)
    return pd.DataFrame(flat_list)

def downloadTaxIDLineage(taxIDs, targetPath, errorDir, allranks = False):
    print("--downloadTaxIDLineage")
    print ("downloading", len(taxIDs), " taxIDs ...")
    URL = "http://bioinfo.icb.ufmg.br/cgi-bin/taxallnomy/taxallnomy_multi.pl?txid="
    if allranks:
        # FULL SET OF AVAILABLE RANKS:
        ranks ="superkingdom,kingdom,subkingdom,superphylum,phylum,subphylum,infraphylum,superclass,class,subclass,infraclass,cohort,subcohort,superorder,order,suborder,infraorder,parvorder,superfamily,family,subfamily,tribe,subtribe,genus,subgenus,section,subsection,series,subseries,species_group,species_subgroup,species,forma_specialis,subspecies,varietas,subvariety,forma,serogroup,serotype,strain,isolate" 
        numRequested = 100
    else:
        ranks ="superkingdom,phylum,class,order,family,genus,species" 
        numRequested = 1000
    
    parameters = "&rank=custom&srank="+ranks+"&format=json"
    
    allDict = {}
    nextOutPut = 10000
    errorIDs = []
    
    for i in range(0, len(taxIDs)):
        queryIDs = taxIDs[i*numRequested: min((i+1)*numRequested, len(taxIDs))] #query <numRequested> taxIDs for the query
        if (len(queryIDs) == 0):
            print ("... break at "+str(i*numRequested))
            break
        
        qURL = URL + ",".join([str(k) for k in queryIDs]) + parameters
        ret = requests.get(qURL)
        ret.text
        try:
            retLineage = json.loads(ret.text)
        except:
            print("ERROR WHITH REQUEST")

        for taxID in retLineage.keys():
            try:
                if "ERROR" not in taxID and "ERROR" not in retLineage[taxID]:
                    allDict[taxID] = retLineage[taxID]
                    allDict[taxID]['taxID'] = taxID
                else:
                    print ("... error with id: ",taxID)
                    errorIDs.append(taxID)
            except:
                print ("... error with id: ",taxID)
                errorIDs.append(taxID)
        
        if (i*numRequested >= nextOutPut ):
            print ("- processed ",str(i*numRequested))
            nextOutPut = nextOutPut + 10000
    
    cols = ['taxID']+ranks.split(",")
    lineageDB = pd.DataFrame.from_dict(allDict).T
    lineageDB = lineageDB[cols]
    pd.DataFrame.to_csv(lineageDB, targetPath, index=False)
    pd.DataFrame.to_csv(pd.DataFrame(errorIDs), errorDir, index=False, header=False)
    print ("- done with ", len(errorIDs), " error-taxIDs")
    return

#krakenReportFile = snakemake.config['workdir'] + snakemake.input[0]
#outputFile = snakemake.config['workdir'] + snakemake.output[0]
#errorFile = snakemake.config['workdir'] + snakemake.output[1]

# INPUT DIRECTORY AND FILE
krakenDir = ""
krakenReportFile = krakenDir+""

# OUTPUT DIRECTORIES AND FILES
workdir = krakenDir+""
outputFile = workdir+""
idFile = workdir+"" # directrory + filename of the file which stores taxonomic IDs
errorFile = workdir+""

# PARSE IDs FROM KRAKEN REPORT
taxIDs = getIDsFromKrakenReport(krakenReportFile, "TAXID") # Second parameter is optional, delete including the comma if default kraken report .this can be commented out if the ID file was created already
pd.DataFrame.to_csv(taxIDs, idFile, index=False) # this can be commented out if the ID file was created already

# LOAD ID FILE IF ALREADY EXISTING
taxIDs = pd.read_csv(idFile)

downloadTaxIDLineage(list(taxIDs.T.values[0]), outputFile, errorFile, allranks = False) # start downloading lineage info for each tax-ID & save the result