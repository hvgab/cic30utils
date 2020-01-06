
"""
	If my memory is correct this script was made to skip a whole lotta steps from our old/usual import job, to just run this script.
	It was specific for 1 client. I believe this was one of the first proof-of-concept codes that I have later used in "purecloud-importer.exe" (with mapping-files between importfile headers and db-headers.)
"""

import os
from time import sleep

import xlrd
import csv

import pymssql

import colorama
import utils
import cic_db as cic_db_lib

colorama.init()
DB_SERVER_CIC = os.getenv('DB_SERVER_CIC')
DB_USER_CIC = os.getenv('DB_USER_CIC')
DB_PASSWORD_CIC = os.getenv('DB_PASSWORD_CIC')
DB_DEFAULT_DB_CIC = os.getenv('DB_DEFAULT_DB_CIC')

class Importjob(object):
	"""docstring for Importjob"""
	def __init__(self, filepath, campaignname):
		#super(Importjob, self).__init__()
		
		self.cic_db = cic_db_lib.CIC_DB(DB_SERVER_CIC, DB_USER_CIC, DB_PASSWORD_CIC, DB_DEFAULT_DB_CIC)
		
		self.filepath = filepath
		#self.filepath = \
		#	utils.rename_slugify_file(self.filepath)
		basepath, filename = os.path.split(self.filepath)
		self.basepath = basepath
		self.filename = filename
		
		self.campaignname = campaignname

		self.wb = xlrd.open_workbook(self.filepath)
		self.sheet = self.wb.sheet_by_index(0)

		self.importnumber = self.next_importnumber()
		self.campaign_data = self.campaign_data()
		self.campaign_cl = self.campaign_data['calllisttable']


	def campaign_data(self):
		q = """
		SELECT *
		FROM DRS_Kampanjer
		WHERE kampanjenavn LIKE '{}'
		""".format(self.campaignname)
		campaign_data = self.cic_db.query_result(q)
		return campaign_data[0]

	def keys_values(self):
		dict_list = []
		keys = self.sheet.row_values(0) # første rad er headers
		values = []
		for rx in range(1, self.sheet.nrows): # values starter på andre rad
			values.append(self.sheet.row_values(rx))
		#for v in values:
		#	dict_list.append(dict(zip(keys, v)))
		return keys, values

	def dict_list(self):
		dict_list = []
		keys, values = self.keys_values()
		for v in values:
			print(len(v))
			for i in range(0, len(v)):
				print (v[i])
				if isinstance(v[i], float):
					if v[i] == int(v[i]):
						v[i] = int(v[i])
				print(v[i])
				#sleep(0.2)
			print(v)
			dict_list.append(dict(zip(keys, v)))
		return dict_list

	def save_as_csv(self):
		keys, values = self.get_keys_values()
		with open('test.csv', 'w') as f:
			writer = csv.writer(f)
			writer.writerow(keys)
			writer.writerows(values)


	def next_importnumber(self):
		campaign_cl = self.campaign_data()['calllisttable']
		q = """
		SELECT MAX (importnumber)+1 AS importnumber
		from {}
		""".format(campaign_cl)
		result = self.cic_db.query_result(q)
		if result[0]['importnumber'] is None:
			return 1
		else:
			return result[0]['importnumber']

	def next_i3_rowid(self, conn):
		q = """
		SELECT MAX(CAST(i3_rowid as int))+1 AS next_i3_rowid
		FROM {}
		""".format(self.campaign_data['calllisttable'])
		result = self.cic_db.query_result(q)
		print(result)
		if result[0]['next_i3_rowid'] is None:
			result[0]['next_i3_rowid'] = '1'
		next_i3_rowid = (str(result[0]['next_i3_rowid']))
		for s in range(len(str(result[0]['next_i3_rowid'])), 9):
			next_i3_rowid = '0{}'.format(next_i3_rowid)
		print (next_i3_rowid)
		return next_i3_rowid

	def insert_row_into_cl(self, row, i3_rowid):

		#INSERT INTO [DialerData].[dbo].[{}] 
		q = """
		INSERT INTO {} 
			([I3_RowID]
			,[ImportNumber]
			,[ImportFilename]
			,[ImportName]
			,[customerNumber]
			,[firstName]
			,[lastName]
			,[adresse]
			,[zip]
			,[city]
			,[phone]
			,[mobile]
			,[email]
			,[emailconfirm]
			
			,[targetAudience]
			,[source]
			,[orgFirstname]
			,[orgLastname]
			,[orgAdresse]
			,[orgZIP]
			,[orgCity]
			,[orgPhone]
			,[orgMobile]
			
			,[ExportBatchNo]
			,[ParticipantID]
			,[AgentNo]
			,[AddressUpdateType]
			,[ConfirmationMedia]
			,[ConfirmationDate]
			,[RejectReason]
			,[TMOptOut]
			,[CustomerMarketId]
			,[ExtraName]
			,[CountryCode]
			,[Gender]
			,[permission_01]
			,[permission_02]
			,[permission_03]
			,[permission_04]
			,[permission_05]
			,[permission_06]
			,[permission_07]
			,[permission_08]
			,[permission_09]
			,[permission_10]
			,[permission_11]
			,[permission_12]
			,[interest_01]
			,[interest_02]
			,[interest_03]
			,[interest_04]
			,[interest_05]
			,[interest_06]
			,[interest_07]
			,[interest_08]
			,[interest_09]
			,[interest_10]
			,[interest_16]
			,[interest_17]
			,[interest_18]
			,[interest_27]
			,[interest_28]
			,[interest_29]
			,[interest_30]
			,[interest_31]
			,[interest_32]
			,[email_01]
			,[email_02]
			,[prod_01]
			,[prod_02]
			,[prod_03]
			,[prod_04]
			,[prod_05]
			,[prod_06]
			,[prod_07]
			,[prod_08]
			,[prod_09]
			,[prod_10]
			,[prod_11]
			,[prod_12]
			,[prod_13]
			,[prod_14]
			,[prod_15]
			,[prod_16]
			,[prod_17]
			,[prod_18]
			,[prod_19]
			,[prod_20]
			,[TargetGroup]
			,[Address_Source]
			,[TM_Permission_source_name]
			,[KDB_ImportGroupId])

			VALUES (
			'{}',{},'{}','{}','{}','{}','{}','{}','{}','{}',
			'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}',
			'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}',
			'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}',
			'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}',
			'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}',
			'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}',
			'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}',
			'{}','{}','{}','{}','{}','{}','{}','{}','{}','{}',
			'{}','{}'
			)
			""".format(
			self.campaign_cl
			,i3_rowid
			,self.importnumber
			,self.filepath # ImportFilename
			,self.filename # ImportName
			,row['personID']
			,row['FirstName']
			,row['LastName']
			,row['Street']
			,row['PostalCode']
			,row['City']
			,row['Phone']
			,row['MobilePhone']
			,row['email_01']
			,row['Email']
			
			,row['TargetGroup'] # target audience
			,row['Address_Source'] # source
			,row['FirstName'] # org firstname
			,row['LastName'] # org lastname
			,row['Street'] # org adresse
			,row['PostalCode'] # org zip
			,row['City'] # org city
			,row['Phone'] # org phone
			,row['MobilePhone'] # org mobile
			
			,row['ExportBatchNo']
			,row['ParticipantID']
			,row['AgentNo']
			,row['AddressUpdateType']
			,row['ConfirmationMedia']
			,row['ConfirmationDate']
			,row['RejectReason']
			,row['TMOptOut']
			,row['CustomerMarketId']
			,row['ExtraName']
			,row['CountryCode']
			,row['Gender']
			,row['permission_01']
			,row['permission_02']
			,row['permission_03']
			,row['permission_04']
			,row['permission_05']
			,row['permission_06']
			,row['permission_07']
			,row['permission_08']
			,row['permission_09']
			,row['permission_10']
			,row['permission_11']
			,row['permission_12']
			,row['interest_01']
			,row['interest_02']
			,row['interest_03']
			,row['interest_04']
			,row['interest_05']
			,row['interest_06']
			,row['interest_07']
			,row['interest_08']
			,row['interest_09']
			,row['interest_10']
			,row['interest_16']
			,row['interest_17']
			,row['interest_18']
			,row['interest_27']
			,row['interest_28']
			,row['interest_29']
			,row['interest_30']
			,row['interest_31']
			,row['interest_32']
			,row['email_01']
			,row['email_02']
			,row['prod_01']
			,row['prod_02']
			,row['prod_03']
			,row['prod_04']
			,row['prod_05']
			,row['prod_06']
			,row['prod_07']
			,row['prod_08']
			,row['prod_09']
			,row['prod_10']
			,row['prod_11']
			,row['prod_12']
			,row['prod_13']
			,row['prod_14']
			,row['prod_15']
			,row['prod_16']
			,row['prod_17']
			,row['prod_18']
			,row['prod_19']
			,row['prod_20']
			,row['TargetGroup']
			,row['Address_Source']
			,row['TM_Permission_source_name']
			,row['KDB_ImportGroupId'])
		return q

def main_import_file(file, campaignname):
	i = Importjob(file, campaignname)
	dict_list = i.dict_list()
	keys_values = i.keys_values()


	num_now = 1
	num_tot = (len(dict_list))

	for row in dict_list:
		i3_rowid = i.next_i3_rowid(i.cic_db.conn)

		for k,v in row.items():
			if isinstance(row[k], str):
				if "'" in row[k]:
					row[k] = row[k].replace("'", "''")
					print(row[k])
					sleep(5)

		q = i.insert_row_into_cl(row, i3_rowid)

		print(row['FirstName'], row['LastName'])
		i.cic_db.query_commit(q)

		perc = float(num_now)/float(num_tot)
		print (perc)
		print (int(perc))
		print('{:.2%}'.format(perc))
		print('{}{}'.format('#'*int(perc*50), '_'*(50-int(perc*50)) ))
		
		# commit for hver 1000 rad?
		#if num_now % 1000 == 0:
		#	i.cic_db.conn.commit()

		num_now += 1

	i.cic_db.conn.commit()
	i.cic_db.conn.close()

def afterwork_import_job():
	#oppdater tlf nr så de blir uten +47
	pass


if __name__ == '__main__':
	file = input('fil?\n\t')
	campaignname = input('campaignname?\n\t')
	main_import_file(file, campaignname)

