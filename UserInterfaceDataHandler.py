# @Author: Nathan North
# @Org: GE Transportation
# @Project: Series X
# Copywrite - all rights reserved. DO NOT COPY OR DISTRIBUTE.

from PredixServices import PredixServices
import time
import datetime
import yaml
import json

class UserInterfaceDataHandler:
    def __init__(self):
        self.configuration = {}
        self.secondConfig = {}
        self.load_configuration('predix-wheel-config.yaml', 'config-stanray.json')
        self.px = PredixServices()

        self.projectStartTimestamp = 1483257600000
        self.wheelTags = []

        self.basicInfo = []

    def load_configuration(self, configFilePath, secondConfigFilePath):
        '''Loads the configuration file'''
        tempConfig = ''
        with open(configFilePath, 'r') as inputFile:
            for line in inputFile:
                tempConfig += line
            self.configuration = yaml.load(tempConfig)

        with open(secondConfigFilePath, 'r') as inputFile:
            self.secondConfig = json.load(inputFile)

        assert len(self.configuration) > 0
        assert len(self.secondConfig) > 0


    def renew_predix_token(self):
        '''Updates the JWT'''
        self.px.uaa_get_token()


    def get_wheel_tags(self):
        '''Pulls the tags from the timeseries instance related to wheels'''
        self.wheelTags = []
        tags = self.px.timeseries_query_tags()
        if tags != None:
            tags = tags['results']
            for cust in self.configuration['Customers']:
                for tag in tags:
                    if tag.endswith(cust):
                        self.wheelTags.append(tag)


    def find_customer(self, tagName):
        for customer in self.configuration['Customers']:
            if tagName.endswith(customer):
                return customer
        return 'Not Found'


    def format_recent_datapoint(self, tempData):
        '''Extracts the most recent datapoint from returned timeseries data'''
        # Perform formatting
        currentTime = datetime.datetime.now()
        firstTag = tempData['tags'][0]
        firstValueSet = firstTag['results'][0]['values'][0]
        firstTime = datetime.datetime.fromtimestamp(firstValueSet[0]/1000)
        currentTimeDiff = currentTime - firstTime
        # Create out dictionary
        formattedData = {}
        formattedData['customer'] = self.find_customer(firstTag['name'])
        formattedData['location'] = self.configuration['Tags'][firstTag['name']]
        formattedData['lastUpdated'] = firstTime.strftime('%A, %b %d, %Y %H:%M:%S')
        formattedData['latestDatapoint'] = firstValueSet[1]
        formattedData['daysElapsed'] = currentTimeDiff.days
        return formattedData


    def pull_most_recent_datapoint(self):
        '''Pulls latest datapoint from timeseries'''
        self.get_wheel_tags()
        currentTime = int(time.time()*1000)
        outData = []
        if len(self.wheelTags) > 0:
            for tag in self.wheelTags:
                tempData = self.px.timeseries_query_data('1y-ago', currentTime, tag, 1)
                outData.append(self.format_recent_datapoint(tempData))
        self.basicInfo = outData
        return outData


    def make_blank_table(self, tableWidth, tableHeight):
        outTable = []

        for i in range(tableHeight):
            tempRow = []
            for i in range(tableWidth):
                tempRow.append('')
            outTable.append(tempRow)
        return outTable


    def make_column_lookup(self, tableName):
        columnLookup = {}
        i = 0
        for item in self.secondConfig['database'][tableName]['columns']:
            columnLookup[str(item['register'])] = i
            i += 1
        return columnLookup


    def determine_data_type(self, register, tableEntries):
        for entry in tableEntries:
            if entry['register'] == register:
                return entry['type']
        return None

    def extract_data_from_results(self, tempData, tableName):
        returnedRegisters = []
        returnedIDs = []
        returnedValues = []

        firstTag = tempData['tags'][0]
        shop = firstTag['name']
        values = firstTag['results'][0]['values']
        for value in values:
            rawData = value[1].split(':')
            retReg = int(rawData[0])
            tableEntries = self.secondConfig['database'][tableName]['columns']
            for item in tableEntries:
                if retReg == item['register']:
                    returnedRegisters.append(retReg)
                    returnedIDs.append(int(rawData[1]))
                    dataType = self.determine_data_type(retReg, tableEntries)
                    if dataType == 'TIMESTAMP':
                        try:
                            value = datetime.datetime.fromtimestamp(int(rawData[2])/1000)
                        except ValueError:
                            value = rawData[2]
                        #returnedValues.append(value.strftime('%A, %b %d, %Y %H:%M:%S'))
                        returnedValues.append(str(value))
                    else:
                        returnedValues.append(rawData[2])

        extractedResults = {}
        extractedResults['shop'] = shop
        extractedResults['returnedRegisters'] = returnedRegisters
        extractedResults['returnedIDs'] = returnedIDs
        extractedResults['returnedValues'] = returnedValues
        return extractedResults


    def enter_data_into_table(self, data, dataTable, columnLookup):
        for i in range(len(data['returnedRegisters'])):
            destinationRow = data['returnedIDs'][i] - 1
            destinationCol = columnLookup[str(data['returnedRegisters'][i])]
            dataTable[destinationRow][destinationCol] = data['returnedValues'][i]
        return dataTable


    def generate_csv_format(self, tempData, tableName):
        csvString = ""
        columnLookup = self.make_column_lookup(tableName)
        data = self.extract_data_from_results(tempData, tableName)
        if len(data['returnedIDs']) > 0:
            tableWidth = len(self.secondConfig['database'][tableName]['columns'])
            tableHeight = max(data['returnedIDs'])

            dataTable = self.make_blank_table(tableWidth, tableHeight)
            dataTable = self.enter_data_into_table(data, dataTable, columnLookup)

            for row in dataTable:
                tempRow = []
                for item in row:
                    tempRow.append(str(item))
                csvString += data['shop'] + ',' + str(tempRow).strip('[]').replace("'", "") + '\n'

        return csvString

    def build_table_header(self, tableName):
        headerList = []
        for item in self.secondConfig['database'][tableName]['columns']:
            headerList.append(str(item['name']))
        csvString = 'Shop,' + str(headerList).strip('[]') + '\n'
        headerString = csvString.replace("'", "")
        return headerString


    def pull_entire_history(self, tableName, tagName=None):
        '''Pulls entire history for a single tag or for all tags if tagName left as None'''
        currentTime = int(time.time()*1000)
        csvDataToReturn = self.build_table_header(tableName)
        if tagName == None:
            # User wants to pull data for all shops

            i = 0
            for tag in self.wheelTags:
                if i == 0:
                    tempData = self.px.timeseries_query_data(self.projectStartTimestamp, currentTime, tag)
                    csvDataToReturn += self.generate_csv_format(tempData, tableName)
                else:
                    tempData = self.px.timeseries_query_data(self.projectStartTimestamp, currentTime, tag)
                    csvDataToReturn += self.generate_csv_format(tempData, tableName)
                i += 1

        else:
            tempData = self.px.timeseries_query_data(self.projectStartTimestamp, currentTime, tagName)
            csvDataToReturn += self.generate_csv_format(tempData, tableName)

        return csvDataToReturn

    def find_tag_name(self, location):
        for tag in self.configuration['Tags']:
            if self.configuration['Tags'][tag] == location:
                return tag
        return None

    def run_export_request(self, requestForm):
        csvData = "Error Gathering Data"
        if requestForm['sel1'] != 'None':
            tableName = requestForm['sel2']
            if 'allCheckBox' in requestForm:
                csvData = self.pull_entire_history(tableName)
            else:
                tagName = self.find_tag_name(requestForm['sel1'])
                print(tagName)
                if tagName != None:
                    csvData = self.pull_entire_history(tableName, tagName)
        return csvData

    def generate_unique_filename(self, requestForm):
        baseName = 'Wheel-True.csv'
        if 'allCheckBox' in requestForm:
            tagName = 'AllShops'
        else:
            tagName = self.find_tag_name(requestForm['sel1'])
        baseName = tagName + '-' + str(requestForm['sel2']) + '-' + baseName
        currentTimeStamp = str(int(time.time()))
        uniqueFilename = currentTimeStamp + '-' + baseName
        return uniqueFilename
