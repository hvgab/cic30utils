"""
	In CIC3.0 every campaign has it's own db table.
	If a project had multiple campaigns, you probably knew the campaignnames of those. And hopefully they had the same table structurce.
	You could use this to quickly find the customer you were looking for and then dive into whatever you needed to do further.
	I guess it was made to quickly figure out which table a customer belonged to.
"""

import os
import pymssql
import pprint
from time import sleep
pp = pprint.PrettyPrinter(indent=2)
import colorama
from colorama import Fore, Back, Style

def search_chcl(campaign_name, where_key, where_val, chcl='cl'):
	if chcl == 'ch': chcl = 'ch0'
	q = """
	select *
	from i3_{c}_{chcl}
	where {k} like '{v}'
	""".format(c=campaign_name, k=where_key, v=where_val, chcl=chcl)
	#print (q)
	cursor.execute(q)
	result = cursor.fetchall()
	print (campaign_name)
	
	#conn.commit()
	return result

def update_finishcode(kampanjenavn, reason, finishcode, where_val):
	q = """
	update ch set reason = '{reason}', finishcode = '{finishcode}'
	from i3_{kampanjenavn}_ch0 ch
	where callid = '{where_val}'

	""".format(reason=reason, finishcode=finishcode, kampanjenavn=kampanjenavn, where_val=where_val)
	cursor.execute(q)
	#result = cursor.fetchall()
	conn.commit()
	#print(q)
	print(Back.GREEN + 'commit')

def update_notat(kampanjenavn, i3_identity, notat):
	q = """
	update cl
	set notat = notat 
	+ ' - '
	+ {notat}'
	from i3_{kampanjenavn}_cl cl
	where i3_identity = {i3_identity}

	""".format(kampanjenavn=kampanjenavn, i3_identity=i3_identity, notat=notat)
	cursor.execute(q)
	#result = cursor.fetchall()
	conn.commit()
	#print(q)
	print(Back.GREEN + 'commit')





def main(campaign_names):

	where_key = 'company_legalname'
	print(Back.BLUE + Style.BRIGHT + 'hva søker vi etter? (husk %)')
	where_val = input('\t')#'%maskinering%'


	for table in campaign_names:
		campaign_name = table
		res = search_chcl(table, where_key, where_val, 'cl')
		
		# print res
		if res: 
			print ('\tres table - ' + Back.CYAN + Style.BRIGHT + '{}'.format(table))
			print ('\timportname: {}'.format(res[0]['ImportName']))
			print ('\tcompany_legalname: ' + Back.CYAN + Style.BRIGHT + '{}'.format(res[0]['company_legalname']))
			print ('\tcompany_orgnr: {}'.format(res[0]['company_orgnr']))
			print ('\tcontact_person_name {}'.format(res[0]['contact_person_name']))
			print ('\taddress_city {}'.format(res[0]['address_city']))
			print ('\taddress_municipality {}'.format(res[0]['address_municipality']))
			print ('\taddress_county {}'.format(res[0]['address_county']))
			
			# print utkodinger
			pp.pprint(res[0]['I3_IDENTITY'])
			ch_res = search_chcl(table, 'i3_identity', res[0]['I3_IDENTITY'], 'ch')
			if ch_res:
				for ch_res_row in ch_res:
					print('\t' + Back.CYAN + ch_res_row['callid'])
					print('\t' + Back.CYAN + ch_res_row['reason'])
					print('\t' + Back.CYAN + ch_res_row['finishcode'])
					print('\t' + Back.CYAN + str(ch_res_row['callplacedtime']))
					print('\n')


			else:
				print(Fore.RED + 'ingen utkoding funnet')
			

			# endre utkoding
			callid_input = input('Hvis du vil endre utkoding, skriv inn callid du vil endre:\n\t')
			if callid_input != '':

				print(Fore.GREEN + 's' + Style.RESET + '=success or ' + Fore.RED + 'f=failure?')
				reasoncode_input = input('\t')
				finishcode = ''
				reasoncode = ''

				# sucess eller failure
				if reasoncode_input == 's':
					reasoncode = 'Success'
					finishcode_input = input('j=ja, o=oppfølging\n\t')
					if finishcode_input == 'j': 
						finishcode = 'JA'
					elif finishcode_input == 'o': 
						finishcode = 'Oppfølging'
					else:
						finishcode = ''
				
				elif reasoncode_input == 'f':
					reasoncode = 'Failure'
					finishcode_input = input('skriv inn nei grunn\n\t')
					if finishcode_input != '':
						finishcode = finishcode_input
				else:
					print(Back.RED + 'Ingen endring valgt.')

				if finishcode != '':
					print('finishcode: {}'.format(finishcode))
					if reasoncode != '':
						print('reasoncode: {}'.format(reasoncode))
						update_finishcode(table, reasoncode, finishcode, callid_input)
						ch_res = search_chcl(table, 'callid', callid_input, 'ch')
						print(Back.GREEN + 'nye koder:')
						print(Back.GREEN + ch_res[0]['reason'])
						print(Back.GREEN + ch_res[0]['finishcode'])
					else:
						print(Back.RED + 'ikke noen reasoncode')
				else:
					print(Back.RED + 'ikke noen finishcode')
				
			# notat
			print('org notat:\n{}'.format(res[0]['notat']))
			
			notat_input = input('Skal det legges til noe på notat? skriv inn...\n\t')

			if notat_input != '':
				update_notat(table, res[0]['I3_IDENTITY'], notat_input)
				notat_res = search_chcl(table, 'i3_identity', res[0]['I3_IDENTITY'], 'cl')
				print(Back.GREEN + 'nytt notat: ')
				print(Back.GREEN + notat_res[0]['notat'])
			else:
				print('notat ikke oppdatert')


if __name__ == '__main__':
	colorama.init(autoreset=True)
	
	server = os.getenv('DB_SERVER')
	user = os.getenv('DB_USER')
	password = os.getenv('DB_PASSWORD')
	conn = pymssql.connect(server, user, password, 'dialerdata')
	cursor = conn.cursor(as_dict=True)
	
	campaign_names = [
		'FILL',
		'IN',
		'CAMPAIGNS'
	]
	
	
	while True:
		main(campaign_names)

	conn.close()
