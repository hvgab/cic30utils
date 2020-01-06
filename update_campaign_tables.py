"""
	Util for CIC dialer.
	Every campaign had it's own table in the db.
	Some times you had to change the table for a campaign. But you might have 50 identical tables, and you had to change everyone of them.
"""

import utils
import settings
import pymssql
from time import sleep

def alter_table(campaign_cl):
	alter = """
			ALTER TABLE [DialerData].[dbo].[{campaign_cl}] 	ALTER COLUMN Address_Source VARCHAR (200) NULL;
			ALTER TABLE [DialerData].[dbo].[{campaign_cl}] 	ALTER COLUMN [source] VARCHAR (200) NULL;
			ALTER TABLE [DialerData].[dbo].[{campaign_cl}] 	ALTER COLUMN TM_Permission_source_name VARCHAR (200) NULL;

			ALTER TABLE [DialerData].[dbo].[{campaign_cl}] 	ALTER COLUMN firstname VARCHAR (75) NULL;
			ALTER TABLE [DialerData].[dbo].[{campaign_cl}]	ALTER COLUMN orgfirstname VARCHAR (75) NULL;
			ALTER TABLE [DialerData].[dbo].[{campaign_cl}]	ALTER COLUMN lastname VARCHAR (75) NULL;
			ALTER TABLE [DialerData].[dbo].[{campaign_cl}] 	ALTER COLUMN orglastname VARCHAR (75) NULL;

			ALTER TABLE [DialerData].[dbo].[{campaign_cl}] 	ALTER COLUMN city VARCHAR (50) NULL;
			ALTER TABLE [DialerData].[dbo].[{campaign_cl}] 	ALTER COLUMN orgcity VARCHAR (50) NULL;

			ALTER TABLE [DialerData].[dbo].[{campaign_cl}] 	ALTER COLUMN adresse VARCHAR (50) NULL;
			ALTER TABLE [DialerData].[dbo].[{campaign_cl}] 	ALTER COLUMN orgadresse VARCHAR (50) NULL;

			ALTER TABLE [DialerData].[dbo].[{campaign_cl}] 	ALTER COLUMN mobile VARCHAR (30) NULL;
			ALTER TABLE [DialerData].[dbo].[{campaign_cl}] 	ALTER COLUMN phone VARCHAR (30) NULL;
			ALTER TABLE [DialerData].[dbo].[{campaign_cl}] 	ALTER COLUMN orgmobile VARCHAR (30) NULL;
			ALTER TABLE [DialerData].[dbo].[{campaign_cl}] 	ALTER COLUMN orgphone VARCHAR (30) NULL;

			ALTER TABLE [DialerData].[dbo].[{campaign_cl}] 	ALTER COLUMN email VARCHAR (320) NULL;
			ALTER TABLE [DialerData].[dbo].[{campaign_cl}] 	ALTER COLUMN emailconfirm VARCHAR (320) NULL;
			ALTER TABLE [DialerData].[dbo].[{campaign_cl}] 	ALTER COLUMN email_01 VARCHAR (320) NULL;
			ALTER TABLE [DialerData].[dbo].[{campaign_cl}] 	ALTER COLUMN email_02 VARCHAR (320) NULL;
			""".format(campaign_cl=campaign_cl)
	return alter


conn = pymssql.connect(settings.DB_SERVER_CIC,
					   settings.DB_USER_CIC,
					   settings.DB_PASSWORD_CIC,
					   settings.DB_DATABASE_CIC)

q_kampanjer = """select *
	from DRS_Kampanjer
	where Kampanjenavn like 'CLIENTNAME%2017'
	"""

kampanjer = utils.query_result(q_kampanjer, conn)

for k in kampanjer:
	print(k['calllisttable'])
	q_alter = alter_table(k['calllisttable'])
	utils.query_commit(q_alter, conn)
