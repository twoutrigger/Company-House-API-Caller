from pymongo import MongoClient, ASCENDING, DESCENDING
from datetime import datetime
from pprint import pprint
import pandas as pd
import numpy as np
import requests
import time

client = MongoClient("") # authorization token removed

db = client.companies_house
companies = db.companies
charges = db.charges
nsics = db.nsics
npostcodes = db.npostcodes

### CH rate limit: 600 requests per 5 minute period per account
# CH tokens removed
tokens = [
    '', '',
]

token = 0

def c_find(x):

    S = requests.Session()
    global token
    global tokens
    sreq = requests.get(f'https://api.companieshouse.gov.uk/company/{str(x)}',
                        auth=(tokens[token], ''))
    token_change = 0
    while sreq.status_code != 200:
        if sreq.status_code == 404: # Not Found API Response
            sdata = None
            break
        else:
            token = ((token + 1)%len(tokens))
            sreq = requests.get(f'https://api.companieshouse.gov.uk/company/{str(x)}',
                                auth=(tokens[token], ''))
            token_change += 1
            if token_change == len(tokens)*2:
                print('Sleeping: ', x)
                time.sleep(300)
                token_change = 0
    else:
        if sreq.status_code == 200:
            sdata = sreq.json()

    return sdata


def c_charge_dynamic(x, start_index):

    S = requests.Session()
    global token
    global tokens
    sreq = requests.get(f'https://api.companieshouse.gov.uk/company/{x}/charges?items_per_page=100&start_index={start_index}',
                        auth=(tokens[token], ''))
    token_change = 0
    while sreq.status_code != 200:
        if sreq.status_code == 404: # Not Found API Response
            sdata = None
            break
        else:
            token = ((token + 1)%len(tokens))
            sreq = requests.get(f'https://api.companieshouse.gov.uk/company/{x}/charges?items_per_page=100&start_index={start_index}',
                                auth=(tokens[token], ''))
            token_change += 1
            if token_change == len(tokens)*2:
                print('Sleeping: ', x)
                time.sleep(300)
                token_change = 0
    else:
        if sreq.status_code == 200:
            sdata = sreq.json()

    return sdata


def clean_comp(x):

    nd = {}
    nd['CompanyName'] = x.get('company_name').upper()
    nd['CompanyNumber'] = str(x.get('company_number'))

    if 'company_status' in x.keys():
        nd['CompanyStatus'] = x.get('company_status').capitalize()

    if ('date_of_creation' in x.keys()) and (x.get('date_of_creation') != ''):
        nd['IncorporationDate'] = datetime.strptime(x.get('date_of_creation'), '%Y-%m-%d')

    if 'type' in x.keys():
        nd['CompanyType'] = x.get('type')

    if 'has_been_liquidated' in x.keys():
        nd['HasBeenLiquidated'] = x.get('has_been_liquidated')

    if 'has_charges' in x.keys():
        nd['HasCharges'] = x.get('has_charges')

    if 'has_insolvency_history' in x.keys():
        nd['HasInsolvencyHistory'] = x.get('has_insolvency_history')

    if 'sic_codes' in x.keys():
        nd['sic_codes'] = x.get('sic_codes')

    if 'registered_office_address' in x.keys():
        nd['registered_office_address'] = x.get('registered_office_address')

    if 'previous_company_names' in x.keys():
        nd['PreviousCompanyNames'] = x.get('previous_company_names')

    if 'accounts' in x.keys():
        nd['Accounts'] = x.get('accounts')

    if 'confirmation_statement' in x.keys():
        nd['ConfirmationStatement'] = x.get('confirmation_statement')

    return nd
