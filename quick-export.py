import pymssql
import settings
import csv
from time import sleep
import pprint
import pandas as pd

pp = pprint.PrettyPrinter(indent=2)

def query_result(q):
  try:
    cursor = conn.cursor(as_dict=True)
    cursor.execute(q)
    result = cursor.fetchall()
    return result
  except:
    result = 'error failure: query_result (failed to connect to db. probably.)'
  return result

def adresse_eksport_query(kampanje):
  print('ch: {ch}\n cl: {cl}\n navn: {kampanjenavn}'.format(ch=kampanje['ch'], cl=kampanje['cl'], kampanjenavn=kampanje['kampanjenavn']))
  return """SELECT
  cl.*
  ,dateofcall = convert(varchar,ch.callplacedtime,104)
  ,timeofcall = left(convert(varchar,ch.callplacedtime,108),5)
  ,ch.reason
  ,ch.finishcode
  ,ch.workflowname
  ,cl.ImportName
  ,ch.AgentID

  from 
  {cl} cl 
  left join 
  (select i3_identity, MAX(callplacedtime) as maxcall 
  from {ch}
  group by i3_identity) chm --chm kun for å finne max
  inner join {ch} ch on chm.i3_identity = ch.i3_identity and chm.maxcall = ch.callplacedtime
  on ch.i3_identity = cl.i3_identity
  order by ch.i3_identity
  """.format(ch=kampanje['ch'], cl=kampanje['cl'], kampanjenavn=kampanje['kampanjenavn'])


def query_format(kampanjenavn, where=''):
  return """SELECT
  cl.*
  ,dateofcall = convert(varchar,ch.callplacedtime,104)
  ,timeofcall = left(convert(varchar,ch.callplacedtime,108),5)
  ,ch.reason
  ,ch.finishcode
  ,ch.workflowname
  ,cl.ImportName

  /*
  ,sms.dialog
  ,sms.Date as sms_date
  ,sms.Message as sms_message
  */
  ,ch.AgentID
  --,agent.SelgerID

  from I3_{kampanjenavn}_cl cl
  inner join I3_{kampanjenavn}_ch0 ch on cl.i3_identity=ch.i3_identity

  /* join for recordname */
  /* alternative 1 */
  left join (select max(callidkey) as recordcallidkey,[RecordedCallIDKey] from [I3_IC].[dbo].[RecordingData] rd
  join [I3_IC].[dbo].[RecordingCall] rc on rc.[RECORDINGID]=rd.[RECORDINGID]
  group by [RecordedCallIDKey])
  rec on rec.[RecordedCallIDKey]=ch.callidkey

  /* alternative 2 */
  /*
  left join [I3_IC].[dbo].[RecordingCall] rc 
  on rc.RecordedCallIDKey = ch.callidkey
  left join [I3_IC].[dbo].[RecordingData] rd
  on rd.RECORDINGID = rc.RECORDINGID
  */

  /* join for sms bekreftelse */
  left join (select * from SMS_Bekreftelser_Dialog
  where 
    dialog in (select dialog from sms_bekreftelser_dialog where Message like '%SMS_SEARCH_TEXT%')
  and Direction = 'in') sms
  on right(sms.[End user address], 8) IN (cl.mobile, cl.phone)
  
  /* join for agentid */
  left join drs_agent agent
  on agent.AgentID = ch.agentid
  """.format(kampanjenavn=kampanjenavn)






def get_cl_ch_tables(kampanjenavn):
  result = query_result("""select
    kampanjenavn, calllisttable as cl, callhistorytable as ch
    from drs_kampanjer
    where Kampanjenavn = '{kampanjenavn}'
    """.format(kampanjenavn=kampanjenavn))
  print('get_cl_ch_tables: \nres:\n{}\nres0:\n{}\n'.format(result, result[0]))
  return result[0]

def get_keys():
  pass


def get_adresse_eksport_from_sql(kampanje):
  print('starter {}'.format(kampanje['kampanjenavn']))
  q = adresse_eksport_query(kampanje)
  print('hentet query')
  result = query_result(q)
  return result

def write_to_file(kampanje, keys):
  with open('{kampanjenavn}_alle_adresser_eksport.csv'.format(kampanjenavn=kampanje['kampanjenavn']), 'w') as csvfile:
      fieldnames = keys
      writer = csv.DictWriter(csvfile, fieldnames=fieldnames, restval='', delimiter=';', lineterminator='\n')
      writer.writeheader()
      print('skriver ut rader')
      for row in result:
        writer.writerow(row)
      print('ferdig med fil')


def select_campaigns():
  kampanjer = []
  flere = True
  while flere:
    kampanje = input('kampanje?\n\t')
    if kampanje == '':
      flere = False
    if flere:
      kampanjer.append(kampanje)
  print ('kampanjer valgt: {}'.format(kampanjer))
  return kampanjer

#########   MAIN
if __name__ == '__main__':
  

  conn = pymssql.connect(settings.DB_SERVER_CIC, settings.DB_USER_CIC, settings.DB_PASSWORD_CIC, settings.DB_DATABASE_CIC)

  # kampanjer
  kampanjer_input = select_campaigns()
  kampanjer = [
    'LIST',
    'OF',
    'CAMPAIGNS',
    'TO',
    'RUN',
    'EXPORT',
    'ON'
  ]
  for k in kampanjer_input:
    print('k: {}'.format(k))
    kk = get_cl_ch_tables(k)
    print('kk: {}'.format(kk))
    kampanjer.append(kk)
    print('appending: {}'.format(kk))
  print('kampanjer: {}'.format(kampanjer))

  same_keys = []
  for kampanje in kampanjer:
    kampanje = get_cl_ch_tables(kampanje)
    print('henter adresse eksport for {}'.format(kampanje))
    adresse_eksport = get_adresse_eksport_from_sql(kampanje)
    print('eksport:')
    print(type(adresse_eksport))
    print(type(adresse_eksport[0]))
    #pp.pprint(adresse_eksport)
    
    print('starter på keys.')
    # keys
    keys_raw = []
    keys_end_of_file = [
      'dateofcall',
      'timeofcall',
      'agentid',
      'reason',
      'finishcode',
      'notat',
      'callbackreason',
      'workflowname',
      'importname',
      'importdate',
      ]
    keys_ignore = [
      'i3_activeworkflowid',
      'i3_attemptsremotehangup',
      'i3_rowid',
      'i3_identity',
      'i3_attemptssitcallable',
      'i3_attemptsbusy',
      'i3_attemptsfax',
      'i3_attemptsmachine',
      'i3_attemptsnoanswer',
      'i3_attemptssystemhangup',
      'i3_attemptsabandoned',
      'i3_siteid',
      'i3_attemptsrescheduled',
      'attempts',
      'zone',
      'phonenumber',
      'counter',
      'moneycounter',
      'importnumber',
      'importfilename',
      'source',
      'targetaudience',
      'couponcode',
      'couponcodeextra',
      'productcode',
      'productcodeextra',
      'prizecode',
      'prizecodeextra',
      'status'
    ]

    """
    for key in adresse_eksport[0]:
      if key.lower() in keys_end_of_file:
        pass
      elif key.lower() in keys_ignore:
        del adresse_eksport[0][key]
        
      else:
        #keys_raw.append(key)
        #print ('key: {}'.format(key))
        pass
    """
    print (len(adresse_eksport))
    i = 0
    while i < len(adresse_eksport)-1:
      i += 1
      for key in list(adresse_eksport[i].keys()):
        if key.lower() in keys_ignore:
          #print ('{} - {}'.format(key, adresse_eksport[i][key]))
          del adresse_eksport[i][key]
        #print ('{} - {}'.format(key, adresse_eksport[0][key]))
        #sleep(1)
    #print (adresse_eksport)
    #sleep(3)

    # sorter keys:
    keys_standard = [
      'customerid',
      'firstname',
      'lastname',
      'mobile',
      'phone',
      'address_street',
      'address_zip',
      'address_city'
    ]
    #keys_sorted = []
    for k in keys_raw:
      if k.lower() in keys_standard:
        keys_raw.remove(k)

    print ('starer på fil eksport:')
    sleep(1)
    
    with open('{}.csv'.format(kampanje['kampanjenavn']), 'w') as csvfile:
      fieldnames = adresse_eksport[0]
      writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
      writer.writeheader()
      writer.writerows(adresse_eksport)
    

    print('ferdig med fil-eksport')

    #write_to_file(kampanje,keys)



  conn.close()
