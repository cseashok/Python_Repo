from websocket import create_connection
import requests
import json
import time
import yaml
import copy

class PredixServices:
    def __init__(self):
        self.configuration = {}
        self.load_configuration('predix-config.yaml')

        self.jwt = ""

        #Asset
        self.assetZoneId = "3cbce81d-c183-4608-8a27-912b18e91fbf"
        self.assetUri = "https://predix-asset.run.aws-usw02-pr.ice.predix.io"
        self.assetEndpoint = ""
        #--- GEL query
        self.gelfilter = ""
        self.gelfiltervalue = ""

        #Timeseries Ingest
        self.tsIngestUrl = "wss://gateway-predix-data-services.run.aws-usw02-pr.ice.predix.io/v1/stream/messages"
        self.tsattributes = {}

        #General stuff
        self.error = '' #Holds the error from any function...

    def load_configuration(self, configFilePath):
        '''Loads the configuration file'''
        tempConfig = ''
        with open(configFilePath, 'r') as inputFile:
            for line in inputFile:
                tempConfig += line
            self.configuration = yaml.load(tempConfig)
        assert len(self.configuration) > 0


    def format_response(self, response):
        try:
            values = json.loads(response.text)
            outVal = (values, False)
        except ValueError:
            # True because there was an error decoding the json
            outVal = (response, True)
        return outVal


    def make_request(self, requestName, payload):
        requestInfo = copy.deepcopy(self.configuration['Request'][requestName])
        requestType = requestInfo['type']
        requestURL = requestInfo['url']
        requestHeader = requestInfo['header']

        if 'authorization' in requestHeader:
            if requestHeader['authorization'] == None:
                # The request needs to be updated with the JWT
                requestHeader['authorization'] = "Bearer " + self.jwt

        if payload == None:
            response = requests.request(requestInfo['type'], requestInfo['url'], headers=requestInfo['header'])
        else:
            response = requests.request(requestInfo['type'], requestInfo['url'], data=payload, headers=requestInfo['header'])
        out = self.format_response(response)
        return out


    def uaa_get_token(self):
        '''Gets JWT and sets it as a class variable to use in other methods'''
        payload = self.configuration['Request']['uaa_get_token']['payload']
        out = self.make_request('uaa_get_token', payload)
        if out[1] == False:
            self.jwt = out[0]['access_token']


    def timeseries_query_tags(self):
        '''Queries the timeseries service for available tags'''
        out = self.make_request('timeseries_query_tags', None)
        if out[1] == False:
            return out[0]
        return None


    def timeseries_query_data(self, startTime, endTime, tagName, limit=None):
        '''Queries your timeseries to get a defined set of values'''
        tags = {
            "name": tagName,
            "order": "desc",
            "limit": limit
        }
        if limit == None:
            tags.pop('limit')

        payload = json.dumps({
            "start":startTime,
            "end":endTime,
            "tags":[tags]
        })
        out = self.make_request('timeseries_query_data', payload)
        if out[1] == False:
            return out[0]
        return None


    def timeseries_make_payload(self, data):
        '''Creates the ingestion payload for timeseries'''
        #Create unique tag name based on the time!
        uniqueId = str(int(time.time()*1000))

        #Build the body of the json request.
        payLoadBody =  {'name':self.tsTagName, 'datapoints':[], 'attributes':self.tsattributes}
        for dataset in data:
            payLoadBody['datapoints'].append(dataset)

        payloadData = {'messageId':uniqueId, 'body':[payLoadBody]}
        MESSAGE = json.dumps(payloadData)
        return MESSAGE


    def timeseries_ingest(self, MESSAGE):
        '''Sends data to a timeseries instance'''
        header = {"Authorization":"Bearer " + self.jwt, "Predix-Zone-Id":self.tsZoneId, 'content-type': "application/json"}
        try:
            ws = create_connection(self.tsIngestUrl, header=header, http_proxy_host="PITC-Zscaler-Americas-Alpharetta3pr.proxy.corporate.ge.com", http_proxy_port=80)
            ws.send(MESSAGE)
            result = ws.recv()
            r = json.loads(result)
            self.error = r['statusCode']
            ws.close()
        except:
            self.error = 'Something went amiss'


    def asset_ingest(self, assetData):
        '''Adds data to the asset service'''
        headers = {
            'content-type': "application/json",
            'predix-zone-id': self.assetZoneId,
            'authorization': "Bearer " + self.jwt,
            }
        response = requests.request("POST", self.assetUri + '/' + str(self.assetEndpoint), data=assetData, headers=headers)
        self.error = response


    def asset_query_data(self):
        '''Queries the asset service for asset data'''
        assetdata = ""
        headers = {
            'content-type': "application/json",
            'predix-zone-id': self.assetZoneId,
            'authorization': "Bearer " + self.jwt,
            }
        response = requests.request("GET", self.assetUri + '/' + str(self.assetEndpoint), headers=headers)
        try:
            assetdata = json.loads(response.text)
        except:
            self.error = response
        return assetdata


    def asset_query_gel(self):
        '''Returns a basic GEL query on the Asset service'''
        assetdata = ""
        headers = {
            'content-type': "application/json",
            'predix-zone-id': self.assetZoneId,
            'authorization': "Bearer " + self.jwt,
            }
        requestURI = self.assetUri + '/' + str(self.assetEndpoint) + '?filter=' + self.gelfilter + '=' + self.gelfiltervalue
        response = requests.request("GET", requestURI, headers=headers)
        try:
            assetdata = json.loads(response.text) #returns a list from gel search
        except:
            self.error = response
        return assetdata
