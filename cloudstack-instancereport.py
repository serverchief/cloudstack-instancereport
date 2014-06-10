#!/usr/bin/python

import urllib2
import urllib
import json
import hmac
import base64
import hashlib
import re
import datetime
import time
import socket
import sys, getopt, argparse
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
try:
    from raven import Client
except:
    pass

def main():
    parser = argparse.ArgumentParser(description='This script connect to cloudstack API, list all vms with details and add  document for each VM in a given ES cluster index')
    parser.add_argument('-version', action='version', version='%(prog)s 1.0, Loic Lambiel exoscale')
    parser.add_argument('-acsurl', help='Cloudstack API URL', required=True, type=str, dest='acsurl')
    parser.add_argument('-acskey', help='Cloudstack API user key', required=True, type=str, dest='acskey')
    parser.add_argument('-acssecret', help='Cloudstack API user secret', required=True, type=str, dest='acssecret')
    parser.add_argument('-esindex', help='ES index name', required=True, type=str, dest='esindex')
    parser.add_argument('-esnodes', help='ES nodes list space separated', required=True, type=str, dest='esnodes')
    parser.add_argument('-sentryapikey', help='Sentry API key', required=False, type=str, dest='sentryapikey')
    args = vars(parser.parse_args())
    return args


class BaseClient(object):
    def __init__(self, api, apikey, secret):
        self.api = api
        self.apikey = apikey
        self.secret = secret

    def request(self, command, args):
        args['apikey']   = self.apikey
        args['command']  = command
        args['response'] = 'json'
        
        params=[]
        
        keys = sorted(args.keys())

        for k in keys:
            params.append(k + '=' + urllib.quote_plus(args[k]).replace("+", "%20"))
       
        query = '&'.join(params)

        signature = base64.b64encode(hmac.new(
            self.secret, 
            msg=query.lower(), 
            digestmod=hashlib.sha1
        ).digest())

        query += '&signature=' + urllib.quote_plus(signature)

        response = urllib2.urlopen(self.api + '?' + query)
        decoded = json.loads(response.read())
       
        propertyResponse = command.lower() + 'response'
        if not propertyResponse in decoded:
            if 'errorresponse' in decoded:
                raise RuntimeError("ERROR: " + decoded['errorresponse']['errortext'])
            else:
                raise RuntimeError("ERROR: Unable to parse the response")

        response = decoded[propertyResponse]
        result = re.compile(r"^list(\w+)s").match(command.lower())

        if not result is None:
            type = result.group(1)

            if type in response:
                return response[type]
            else:
                # sometimes, the 's' is kept, as in :
                # { "listasyncjobsresponse" : { "asyncjobs" : [ ... ] } }
                type += 's'
                if type in response:
                    return response[type]

        return response

    def listVirtualMachines(self, args={}):
        return self.request('listVirtualMachines', args)


def get_stats(args):

    ACSURL = args['acsurl']
    ACSKEY = args['acskey']
    ACSSECRET = args['acssecret']
    esindex = args['esindex']
    esnodes = args['esnodes']
    try:
        SENTRYAPIKEY = args['sentryapikey']
    except:
        pass

    DOCTYPE = 'acs-vm-reporting'    

    now = time.strftime("%Y.%m.%d")
    esindex = esindex + '-' + now

    # collect number of virtual machines
    cloudstack = BaseClient(ACSURL, ACSKEY, ACSSECRET)

    query_tmp = None
    querypage = 1
    querypagesize = 500
    virtualmachines = cloudstack.listVirtualMachines({
        'listall': 'true',
        'details': 'all',
        'page': str(querypage),
        'pagesize': str(querypagesize)
        })
    all_virtualmachines = []
    if len(virtualmachines) == querypagesize:
        query_tmp = virtualmachines
        while len(query_tmp) > 0:
            all_virtualmachines.extend(query_tmp)
            querypage = querypage + 1
            query_tmp = cloudstack.listVirtualMachines({
                            'listall': 'true',
                            'details': 'all',
                            'page': str(querypage),
                            'pagesize': str(querypagesize)
                            })
    else:
        all_virtualmachines.extend(virtualmachines)
    virtualmachines = all_virtualmachines

    ESCLUSTERNODES = esnodes.split()
    es = Elasticsearch(ESCLUSTERNODES)
    
    timestamp = datetime.datetime.now()
    #create ES mappings
    es.indices.put_mapping(
        index=esindex,
        doc_type=DOCTYPE,
            body={
                DOCTYPE: {
                    'properties': {
                        '@fields.account': {
                            'type': 'string'
                            },
                        '@fields.id': {
                            'type': 'string',
                            'index': 'not_analyzed'
                            },
                        '@fields.state': {
                            'type': 'string',
                            'index': 'not_analyzed'
                            },
                        '@fields.cpunumber': {
                            'type': 'string',
                            'index': 'not_analyzed'
                            },
                        '@fields.displayname': {
                            'type': 'string'
                            },
                        '@fields.hostname': {
                            'type': 'string'
                            },
                        '@fields.instancename': {
                            'type': 'string'
                            },
                        '@fields.memory': {
                            'type': 'string',
                            'index': 'not_analyzed'
                            },
                        '@fields.name': {
                            'type': 'string'
                            },
                        '@fields.serviceofferingid': {
                            'type': 'string',
                            'index': 'not_analyzed'
                            },
                        '@fields.serviceofferingname': {
                            'type': 'string'
                            },
                        '@fields.templateid': {
                            'type': 'string',
                            'index': 'not_analyzed'
                            },
                        '@fields.templatedisplaytext': {
                            'type': 'string'
                            },
                        '@fields.zoneid': {
                            'type': 'string',
                            'index': 'not_analyzed'
                            },
                        '@fields.ipaddress': {
                            'type': 'string',
                            'index': 'not_analyzed'
                            },
                        '@fields.macaddress': {
                            'type': 'string',
                            'index': 'not_analyzed'
                            },
                        '@fields.securitygroupid': {
                            'type': 'string',
                            'index': 'not_analyzed'
                            },
                        '@fields.securitygroupname': {
                            'type': 'string'
                            },
                        '@source_host': {
                            'type': 'string'
                            },
                        '@type': {
                            'type': 'string'
                            },
                        '@message': {
                            'type': 'string'
                            },
                        '@timestamp': {
                            'type': 'date'
                            }
                    }
                }
            }
    )  
    #create ES document
    if virtualmachines:
        records = []
        for virtualmachine in virtualmachines:
            vmaccount = virtualmachine['account']
            vmid = virtualmachine['id']
            vmstate = virtualmachine['state']
            vmcpunumber = virtualmachine['cpunumber']
            vmdisplayname = virtualmachine['displayname']
            vmhostname = virtualmachine['hostname']
            vminstancename = virtualmachine['instancename']
            vmmemory = virtualmachine['memory']
            vmname = virtualmachine['name']
            vmserviceofferingid = virtualmachine['serviceofferingid']
            vmserviceofferingname = virtualmachine['serviceofferingname']
            vmtemplateid = virtualmachine['templateid']
            vmtemplatedisplaytext = virtualmachine['templatedisplaytext']
            vmzoneid = virtualmachine['zoneid']
            vmzonename = virtualmachine['zonename']
            for nic in virtualmachine['nic']:
                vmipaddress = nic['ipaddress']
                vmmacaddress = nic['macaddress']
            for securitygroup in virtualmachine['securitygroup']:
                vmsecuritygroupid = securitygroup['id']
                vmsecuritygroupname = securitygroup['name']
            sourcehost = socket.gethostname()
            #body
            doc = {
                '@fields.account': vmaccount,
                '@fields.id': vmid,
                '@fields.state': vmstate,
                '@fields.cpunumber': vmcpunumber,
                '@fields.displayname': vmdisplayname,
                '@fields.hostname': vmhostname,
                '@fields.instancename': vminstancename,
                '@fields.memory': vmmemory,
                '@fields.name': vmname,
                '@fields.serviceofferingid': vmserviceofferingid,
                '@fields.serviceofferingname': vmserviceofferingname,
                '@fields.templateid': vmtemplateid,
                '@fields.templatedisplaytext': vmtemplatedisplaytext,
                '@fields.zoneid': vmzoneid,
                '@fields.zonename': vmzonename,
                '@fields.ipaddress': vmipaddress,
                '@fields.macaddress': vmmacaddress,
                '@fields.securitygroupid': vmsecuritygroupid,
                '@fields.securitygroupname': vmsecuritygroupname,
                '@source_host': sourcehost,
                '@type': 'Cloudstack-vm-reporting',
                '@message': 'CS instance report for vm %s' % vmname,
                '@timestamp': timestamp
            }
            records.append(doc)

        #index documents
        bulk(es, records, index=esindex, doc_type=DOCTYPE)

#main
if __name__ == "__main__":
    args = main()
    try:
        get_stats(args)
    except Exception:
        if args['sentryapikey'] == 'None':
            client = Client(dsn=args['sentryapikey'])
            client.captureException()
        else:
            raise


