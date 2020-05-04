from pymongo import MongoClient, ASCENDING, DESCENDING
from func import c_find, c_charge_dynamic, clean_comp
from datetime import datetime, timedelta
from pprint import pprint
import pandas as pd
import numpy as np
import requests
import time

#################################################

### API Caller to update companies+charges tables

client = MongoClient("") #authentication token removed

db = client.companies_house
companies = db.companies
charges = db.charges
officers = db.officers
people = db.people
nsics = db.nsics
npostcodes = db.npostcodes

#################################################

### CODE START

# comp_remain = companies.count_documents( { 'UpdateItem': { '$lt': (datetime.today() - timedelta(days=7)) } } )
# print(f'COMPANY ENTRIES OLDER THAN ONE WEEK: {comp_remain}')
# '''

companies_count = 10000

print(f'UPDATING {companies_count} ENTRIES')

t0 = time.time()
counter = 0
db_skip = 0
db_limit = 500

while counter < companies_count:

    # This version updates HasCharges == True
    companies_cursor = companies.find( { 'HasCharges': True }, { '_id': 0 } ).sort('UpdateItem', ASCENDING).skip(db_skip).limit(db_limit)

    for doc in companies_cursor:

        try:

            cn = doc.get('CompanyNumber')

            sfind = c_find(cn)

            if sfind is None:

                companies.delete_one( { 'CompanyNumber': cn } )
                charges.delete_many( { 'CompanyNumber': cn } )
                people.delete_many( { 'CompanyNumber': cn } )
                officers.delete_many( { 'CompanyNumber': cn } )

            else:

                sclean = clean_comp(sfind)

                # UPDATE CHARGES
                if sclean.get('HasCharges') == True:

                    processed_count = 0

                    schargecount = c_charge_dynamic(cn, processed_count)

                    if schargecount is not None:

                        if 'unfiltered_count' in schargecount:

                            sclean['Mortgages'] = {}
                            sclean['Mortgages']['NumMortCharges'] = schargecount.get('unfiltered_count')
                            sclean['Mortgages']['NumMortSatisfied'] = schargecount.get('satisfied_count')
                            sclean['Mortgages']['NumMortPartSatisfied'] = schargecount.get('part_satisfied_count')
                            sclean['Mortgages']['NumMortOutstanding'] = schargecount.get('unfiltered_count') - (schargecount.get('satisfied_count') + schargecount.get('part_satisfied_count'))

                            if 'Mortgages' in doc:
                                if sclean['Mortgages'] == doc['Mortgages']:

                                    ### Update UpdateItem only
                                    charges.update_many( { 'CompanyNumber': cn },
                                        { '$set' : { 'UpdateItem': datetime.now().replace(minute=0, second=0, microsecond=0) } } )

                                    processed_count = sclean.get('Mortgages').get('NumMortCharges')

                            while processed_count < sclean.get('Mortgages').get('NumMortCharges'):

                                for s in schargecount.get('items'):

                                    if 'links' in s:

                                        scharge = s
                                        scharge['CompanyNumber'] = cn
                                        scharge['id'] = scharge.get('links').get('self').split('/')[-1]
                                        if 'created_on' in scharge:
                                            scharge['created_on'] = datetime.strptime(scharge.get('created_on'), '%Y-%m-%d')
                                        if 'delivered_on' in scharge:
                                            scharge['delivered_on'] = datetime.strptime(scharge.get('delivered_on'), '%Y-%m-%d')
                                        if 'satisfied_on' in scharge:
                                            scharge['satisfied_on'] = datetime.strptime(scharge.get('satisfied_on'), '%Y-%m-%d')

                                        scharge_db = charges.find_one( { 'CompanyNumber': cn, 'id': scharge.get('id') }, { '_id': 0 } )

                                        if scharge_db is not None:
                                            if 'UpdateItem' in scharge_db:
                                                del scharge_db['UpdateItem']
                                            if '__v' in scharge_db:
                                                del scharge_db['__v']
                                            if 'persons_cleaned' in scharge_db:
                                                scharge['persons_cleaned'] = scharge_db.get('persons_cleaned')

                                        if scharge == scharge_db:

                                            ### Find and Update Charge
                                            charges.find_one_and_update( { 'CompanyNumber': cn, 'id': scharge.get('id') },
                                                { '$set' : { 'UpdateItem': datetime.now().replace(minute=0, second=0, microsecond=0) } }, upsert=True )

                                        else:

                                            scharge['UpdateItem'] = datetime.now().replace(minute=0, second=0, microsecond=0)

                                            ### Find and Replace Charge
                                            charges.find_one_and_replace( { 'CompanyNumber': cn, 'id': scharge.get('id') }, scharge, upsert=True )

                                    processed_count += 50

                                    schargecount = c_charge_dynamic(cn, processed_count)

                if 'sic_codes' in sclean:
                    if 'SICCode' in doc:
                        if set(sclean.get('sic_codes')) == set([updated_sic.get('code') for updated_sic in doc.get('SICCode')]):

                            sclean['SICCode'] = doc.get('SICCode')

                        else:
                            sic_build = []
                            for sic in sclean.get('sic_codes'):
                                sic_found =  nsics.find_one( { 'SIC': sic } )
                                if sic_found is not None:
                                    if len(sic_found.get('SIC')) == 5:
                                        sic_build.append( {'code': sic_found.get('SIC'), 'description': sic_found.get('OUTPUT') } )
                                    elif len(sic_found.get('SIC')) == 4:
                                        sclean['HasOldSic'] = True
                                        for sic_old in sic_found.get('OUTPUT').split(' '):
                                            sic_new = nsics.find_one( { 'SIC': sic_old } )
                                            sic_build.append( {'code': sic_new.get('SIC'), 'description': sic_new.get('OUTPUT') } )
                                else:
                                    sic_build.append( {'code': sic, 'description': 'UNKNOWN' } )

                            if len(sic_build) > 0:
                                sclean['SICCode'] = sic_build

                    del sclean['sic_codes']

                if 'registered_office_address' in sclean:
                    if 'RegAddress' in doc:
                        if set(('address_line_1', 'postal_code')).issubset(sclean['registered_office_address']) & \
                            set(('AddressLine1', 'PostCode')).issubset(doc['RegAddress']):
                            if (sclean['registered_office_address']['address_line_1'].upper() == doc['RegAddress']['AddressLine1'].upper()) & \
                                (sclean['registered_office_address']['postal_code'].upper() == doc['RegAddress']['PostCode'].upper()):

                                sclean['RegAddress'] = doc.get('RegAddress')

                            else:
                                sclean['RegAddress'] = {}
                                if 'address_line_1' in sclean['registered_office_address'].keys():
                                    sclean['RegAddress']['AddressLine1'] = sclean.get('registered_office_address').get('address_line_1').upper()
                                if 'address_line_2' in sclean['registered_office_address'].keys():
                                    sclean['RegAddress']['AddressLine2'] = sclean.get('registered_office_address').get('address_line_2').upper()
                                if 'locality' in sclean['registered_office_address'].keys():
                                    sclean['RegAddress']['PostTown'] = sclean.get('registered_office_address').get('locality').upper()
                                if 'postal_code' in sclean['registered_office_address'].keys():
                                    sclean['RegAddress']['PostCode'] = sclean.get('registered_office_address').get('postal_code').upper()

                                    post_found = npostcodes.find_one( { 'Postcode': sclean.get('registered_office_address').get('postal_code').upper() } )
                                    if post_found is not None:
                                        sclean['RegAddress']['Country'] = post_found.get('Country').upper()
                                        sclean['RegAddress']['District'] = post_found.get('District').upper()
                                        sclean['RegAddress']['Coord'] = { 'type': 'point' , 'coordinates': post_found.get('coordinates') }
                                        if 'Region' in post_found.keys():
                                            sclean['RegAddress']['Region'] = post_found.get('Region').upper()
                                    else:
                                        if 'country' in sclean['registered_office_address'].keys():
                                            sclean['RegAddress']['Country'] = sclean.get('registered_office_address').get('country').upper()
                                        else:
                                            sclean['RegAddress']['Country'] = 'UNKNOWN'

                    del sclean['registered_office_address']

                sclean['UpdateItem'] = datetime.now().replace(minute=0, second=0, microsecond=0)

                ### Find and Replace Company
                companies.find_one_and_replace( { 'CompanyNumber': cn }, sclean )

        except:
            print(f'Company Error: {cn}')
            db_skip += 1
            pass

        counter += 1

t1 = time.time()
print(f'UPDATE FINISHED - Time: {int((t1-t0)/60)} minutes; Seconds per entry: {round((t1-t0)/ counter, 2)}')
print('Three minute break...')
time.sleep(180)
# '''
