import sys
import time
import os
import logging
from logging.config import dictConfig
import getopt
from configobj import ConfigObj
from datetime import datetime
from datetime import timedelta

import numpy as np
import pandas as pd
import csv
import unicodedata
import re
import ipaddress

from ftplib import FTP
import codecs
import binascii
import gzip
import multiprocessing
import threading
import queue

import requests
import json

###############################################################################
# pip install --upgrade pip
# pip install --upgrade pandas
# pip install --upgrade requests
# pip install --upgrade configobj
# ftp://ftp.ripe.net/ripe/dbase/split/ripe.db.inetnum.gz
###############################################################################

###############################################################################
# Globals
###############################################################################
thisname='ipsearch'

###############################################################################
def update_db():

    """ get the gzipped ripe database and save it to disk """
    # ftp://ftp.ripe.net/ripe/dbase/split/ripe.db.inetnum.gz
    ftp = FTP('ftp.ripe.net')
    ftp.login()
    ftp.retrbinary('RETR ripe/dbase/split/ripe.db.inetnum.gz',open('ripe.db.inetnum.gz','wb').write)
    ftp.quit()

###############################################################################
def search_data(data,query_params,out_q):
    # pat = re.compile('|'.join(map(re.escape, ['RADISSON EDWARDIAN'])), re.IGNORECASE)
    query_words=query_params
    query_search=[ x.replace(' ','.') for x in query_words ]
        
    pat = re.compile('|'.join(query_search), re.IGNORECASE)
    match = pat.search(data)

    match_list=[]    
    while match:
        span = match.span()

        entry_start = data[:span[0]].rfind('\n\n')
        if entry_start < 0:
            entry_start = 0
        entry_stop = span[1] + data[span[1]:].find('\n\n')
        if entry_stop <= 0:
            entry_stop = span[1]

        # Extract ip_start, ip_end, netname, country, descr1,descr2,descr3,descr4,descr5
        # from inetnum block
        add_match={}
        descr=''
        for line in data[entry_start:entry_stop].splitlines():
            
            if line.startswith('inetnum'):
                content = line.strip().replace(" ", "").split(":")[1]
                ip_start=ipaddress.ip_address(content.split('-')[0])
                ip_end=ipaddress.ip_address(content.split('-')[1])
                range_length=int(ip_end)-int(ip_start)+1
                add_match['ip_start']=ip_start
                add_match['ip_end']=ip_end
                add_match['range_length']=range_length

            if line.startswith('netname'):
                content = line.strip().replace(" ", "").split(":")[1]
                add_match['netname']=content
                
            if line.startswith('country'):
                content = line.strip().replace(" ", "").split(":")[1]
                add_match['country']=content
                
            if line.startswith('descr'):
                content = line.split(":")[1]
                descr=descr + ' ' + content
        descr='[RIPE] '+descr
        add_match['descr']=descr.strip(' ')
        match_list.append(add_match)
        
        data = data[entry_stop:]
        match = pat.search(data)

    df_res=pd.DataFrame(match_list)
    out_q.put(df_res)
    if(len(match_list)):
        logger.info('FUNC search_data: Found {} match in chunk'.format(len(match_list)))

###############################################################################
def is_gz_file(filepath):
    with open(filepath, 'rb') as test_f:
        return binascii.hexlify(test_f.read(2)) == b'1f8b'

###############################################################################
def nets2dict(nets,org_handle,org_name):
    add_match={}
    ip_start=ipaddress.ip_address(nets['@startAddress'])
    if(ip_start.version==6):
        return(add_match)
    ip_end=ipaddress.ip_address(nets['@endAddress'])
    range_length=int(ip_end)-int(ip_start)+1
    add_match['ip_start']=ip_start
    add_match['ip_end']=ip_end
    add_match['range_length']=range_length
    add_match['netname']=nets['@handle']
    add_match['country']='US'
    add_match['descr']='[ARIN] '+org_handle+' '+org_name+' '+nets['@name']
    logger.info('add_match={}'.format(add_match))
    return(add_match)

###############################################################################
def main():
    logger.info('-------------------------------------------------------------------')
    logger.info('Start '+thisname)

    logger.info('Params list=[{}]'.format(params))
    logger.info('Config list=[{}]'.format(config))

    # query_words=['RADISSON']
    # query_words=['PEUGEOT']
    # query_words=['RENAULT']
    # query_words=['CITROEN']
    # query_words=['KRAFT FOODS']
    query_words=[params['company_name']]
    df_search=pd.DataFrame()    

    # Output file
    fname_search_out=datetime.now().strftime('%Y%m%d')+'_'+re.sub(r"\s+", '_', re.sub(r"[^\w\s]",'',params['company_name'])).upper()+'_SEARCH.csv'
    logger.info('Sub fname_search_out={}'.format(fname_search_out))

    ##################################################################################
    ## ARIN SEARCH
    ##################################################################################    
    query_search=[ '*'+x.replace(' ','*')+'*' for x in query_words ]
    logger.info('query_search={}'.format(query_search))

    url_search_org='http://whois.arin.net/rest/orgs;name={}'
    url_search_org_nets='http://whois.arin.net/rest/org/{}/nets'
    url_search_cust='http://whois.arin.net/rest/customers;name={}'
    url_search_cust_nets='http://whois.arin.net/rest/customer/{}/nets'

    match_list=[]  
    for qsearch in query_search:
        r_org=requests.get(url_search_org.format(qsearch), headers={'Accept':'application/json'})
        if(r_org):
            j_org=json.loads(r_org.content.decode('utf-8'))['orgs']['orgRef']
        else:
            j_org=[]
                    
        if (type(j_org) != list):
            j_org=[j_org]
        for org in j_org:
            org_handle=org['@handle']
            org_name=org['@name']
            r_nets=requests.get(url_search_org_nets.format(org_handle),headers={'Accept':'application/json'})
            if (r_nets.ok):
                j_nets=json.loads(r_nets.content.decode('utf-8'))['nets']['netRef']
                if (type(j_nets) != list):
                    j_nets=[j_nets]
                for nets in j_nets:
                    add_match=nets2dict(nets,org_handle,org_name)
                    if(len(add_match)>0):
                        match_list.append(add_match)

        r_cust=requests.get(url_search_cust.format(qsearch), headers={'Accept':'application/json'})
        if(r_cust):        
            j_cust=json.loads(r_cust.content.decode('utf-8'))['customers']['customerRef']
        else:
            j_cust=[]
            
        if (type(j_cust) != list):
            j_cust=[j_cust]
        for cust in j_cust:
            cust_handle=cust['@handle']
            cust_name=cust['@name']
            r_nets=requests.get(url_search_cust_nets.format(cust_handle),headers={'Accept':'application/json'})
            if (r_nets.ok):
                j_nets=json.loads(r_nets.content.decode('utf-8'))['nets']['netRef']
                if (type(j_nets) != list):
                    j_nets=[j_nets]
                for nets in j_nets:
                    add_match=nets2dict(nets,cust_handle,cust_name)
                    if(len(add_match)>0):
                        match_list.append(add_match)
    df_res=pd.DataFrame(match_list)

    df_search=pd.concat([df_search,df_res], ignore_index=True)

    ##################################################################################
    ## RIPE SEARCH
    ##################################################################################    
    ripedb_fname='/Users/llejariel/Devenv/wlpoc'+'/'+'ripe.db.inetnum.gz'
    gz_ripedb=is_gz_file(ripedb_fname)

    with open(ripedb_fname,'rb') as f_ripe:
        if(gz_ripedb):
            logger.info('Gzipped file')
            f_ripe = gzip.GzipFile(fileobj=f_ripe)
        else:
            logger.info('Not Gzipped file')

        # data=f_ripe.read(500).decode('ISO-8859-1')
        # logger.info('data={}'.format(data))
        pos = 0
        out_q = multiprocessing.Queue()
        data_buf = ''
        batchsize=15791394
        nthreads=16
        chunk = int(batchsize / nthreads)
        done = False

        while not done:
            logger.info('Processed {} bytes'.format(pos))
            procs = []
            
            # read data to feed n threads
            for i in range(nthreads):
                # we may have some data buffered from the previous round
                data = data_buf
                l_buf = len(data_buf)
                data_buf = ''
                try:
                    data += f_ripe.read(chunk - l_buf).decode('ISO-8859-1')
                except:
                    done = True
                if not data:
                    done = True
                    break
                l_buf = 0

                # don't forget to clear the buffer
                # read until entry is complete, so we don't have
                # to deal with searching inet nums across
                # multiple data chunks
                while not done and data[-2:] != '\n\n':
                    try:
                        data_buf += f_ripe.read(1024).decode('ISO-8859-1')
                    except:
                        done = True
                        break
                    if not data_buf:
                        done = True
                        break
                    idx = data_buf.find('\n\n')
                    # we have found the end of our entry
                    if idx >= 0:
                        data += data_buf[:idx + 2]
                        data_buf = data_buf[idx + 2:]
                        break
                pos += len(data)
                if data:
                    p = multiprocessing.Process(target=search_data, args=[data,query_words,out_q])
                    procs.append(p)
                    p.start()
                if done:
                    break

            # join threads so that we dont flood the ram
            # get results:
            while True:
                try:
                    df_res=out_q.get(False)
                    df_search=pd.concat([df_search,df_res], ignore_index=True)
                except queue.Empty:
                    break

            for p in procs:
                p.join()

    df_search=df_search.sort_values(by='ip_start')
    df_search=df_search.reset_index(drop=True)
    logger.info('FUNC main: df_search={}'.format(df_search))

    ######################################################################################
    # Final output
    col_search=[
            'netname',
            'descr',
            'country',
            'ip_start',
            'ip_end',
            'range_length']
    if (not df_search.empty):
        df_search.to_csv(fname_search_out,columns=col_search,index=False)

    logger.info('Done '+thisname)
    logger.info('-------------------------------------------------------------------')

## -------------------------------------------------------------------
## ---- Tech functions

## -------------------------------------------------------------------
## init_log():
def init_log():
    ## Logging init
    logging_config = { 
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': { 
            'standard': {'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'}
        },
        'handlers': { 
            'ch': {'level': 'INFO','formatter': 'standard','class': 'logging.StreamHandler'},
            'fh': {'level': 'INFO','formatter': 'standard','class': 'logging.handlers.RotatingFileHandler',
                   'filename': thisname+'.log','mode': 'a','encoding': 'utf-8',
                   'maxBytes': 10485760,'backupCount': 5,}
       },
        'loggers': {
            '': {'handlers': ['ch','fh'],'level': 'INFO','propagate': True},
        }
    }
    logging.config.dictConfig(logging_config)
    logging_init=logging.getLogger()
    return(logging_init)

## -------------------------------------------------------------------
## read_cli():
def read_cli():
    help_str=thisname+'.py -p <param_name>'
    params={'company_name':''}
    logger.info('Reading params')
    try:
        opts, args = getopt.getopt(sys.argv[1:],"hc:",["company_name="])
    except getopt.GetoptError as opterr:        
        logger.error(opterr)
        logger.error(help_str)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            logger.error(help_str)
            sys.exit()
        elif opt in ("-c", "--company_name"):
            params['company_name'] = arg
            
    logger.info('In read_cli list=[{}]'.format(params['company_name']))
    return(params)

## -------------------------------------------------------------------
## read_config():
def read_config():
    cwd=os.getcwd()
    return(ConfigObj(cwd+'/python_run_prod.env'))

## -------------------------------------------------------------------
## Main procedure
if __name__ == '__main__':
    logger=init_log()
    config=read_config()
    params=read_cli()

    main()
